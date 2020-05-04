import os
import pandas as pd
import numpy as np
from datetime import datetime as dt
import math
import random
from collections import defaultdict, namedtuple
import sys
from tqdm import tqdm
import json
import argparse
from hyperopt import hp, fmin, tpe, Trials, STATUS_OK
from functools import partial
from queue import Queue
from threading import Thread, Lock
import matplotlib as plt
import seaborn as sb

MIN_REC = 0.1
MAX_REC = 1.0 
MIN_HL = 0.1 #days
MAX_HL = 90.0 #days
MIN_WEIGHT = 0.0001
HYPER_PARAM_OPT_ROUNDS = 50
LN2 = math.log(2.)
REC_MULTIPLIER = 2.5 
# For docker 
category_to_chapter_map = defaultdict(int)
category_to_chapter_map.update(json.load(open(os.path.join('app', 'category_chapter_map.json'))))
chapter_to_subject_map = defaultdict(int)
chapter_to_subject_map.update(json.load(open(os.path.join('app', 'chapter_subject_map.json'))))

PRETRAINED_WEIGHTS = dict(chapter=os.path.join('app', 'chapter_weights.json'), subject=os.path.join('app', 'subject_weights.json'))
"""

# For training
category_to_chapter_map = defaultdict(int)
category_to_chapter_map.update(json.load(open(os.path.join('.', 'category_chapter_map.json'))))
chapter_to_subject_map = defaultdict(int)
chapter_to_subject_map.update(json.load(open(os.path.join('.', 'chapter_subject_map.json'))))

PRETRAINED_WEIGHTS = dict(chapter=os.path.join('.', 'chapter_weights.json'), subject=os.path.join('.', 'subject_weights.json'))

"""
WORKERS = 6 

Instance = namedtuple('Instance', ['userid', 'recall', 'hl', 'time_delta', 'entityid', 'last_practiced_at', 'feature_vector'])
Result = namedtuple('Result', ['userid', 'entityid', 'last_practiced_at', 'recall', 'hl'])

inference_model = None

hyper_param_space = {
						'lrate': hp.loguniform('lrate', np.log(.0001), np.log(.2)), 
						'hlwt': hp.loguniform('hlwt', np.log(0.001), np.log(0.5)), 
						'l2wt': hp.loguniform('l2wt', np.log(0.001), np.log(0.2)), 
						'sigma': hp.uniform('sigma', 0., 1.)
					}

def to_days(millis):
	return (millis / 1000.0) / 86400.0


def to_minutes(millis):
	return (millis / 1000.0) / 60.0


def recall_clip(recall):
	return min(max(MIN_REC, recall), MAX_REC)


def halflife_clip(halflife):
	return min(max(MIN_HL, halflife), MAX_HL)


def mae(l1, l2):
	# mean average error
	return mean([abs(l1[i] - l2[i]) for i in range(len(l1))])


def mean(lst):
	# the average of a list
	return float(sum(lst)) / (len(lst) + 0.001)


def spearmanr(l1, l2):
	# spearman rank correlation
	m1 = mean(l1)
	m2 = mean(l2)
	num = 0.
	d1 = 0.
	d2 = 0.
	for i in range(len(l1)):
		num += (l1[i]-m1) * (l2[i]-m2)
		d1 += (l1[i]-m1) ** 2
		d2 += (l2[i]-m2) ** 2
	try:
		return num/math.sqrt(d1 * d2)
	except ZeroDivisionError:
		return -1


def get_recall(hl, lag):
	return 2 ** (-lag/hl)


def get_hl_in_practice(recall, calculated_hl):
	return max(MIN_HL, math.exp(recall * REC_MULTIPLIER) * calculated_hl)


def get_instances(df, is_training_phase, working_at_subject_level = False, last_practiced_map = None):
	print ("Reading data... {}".format(len(df)))
	instances = list()
	for groupid, groupdf in df.groupby(['userid', 'examid', 'subjectid' if working_at_subject_level else 'chapterid']):
		userid = groupid[0]
		examid = groupid[1]
		entityid = groupid[2]
		correct_attempts_all = 0
		total_attempts_all = 0
		prev_session_end = None if not last_practiced_map else last_practiced_map.get(int(entityid), None)
		for sessionid, session_group in groupdf.groupby(['date']):
			session_name = "{}".format(sessionid)
			total_attempts = len(session_group)
			total_attempts_all += total_attempts
			correct_df = session_group[session_group['iscorrect'] == True]
			correct_attempts = len(correct_df)
			correct_attempts_all += correct_attempts
			actual_recall = recall_clip(float(correct_df['difficulty'].sum()) / session_group['difficulty'].sum())
			session_begin_time = session_group['attempttime'].min()
			if is_training_phase and not prev_session_end:
				prev_session_end = session_group['attempttime'].max()
				continue
			time_delta = MAX_HL if not prev_session_end else to_days(session_begin_time - prev_session_end)
			actual_halflife = MAX_HL if not prev_session_end else halflife_clip(-time_delta / math.log(actual_recall, 2))
			prev_session_end = session_group['attempttime'].max()

			feature_vector = list()
			feature_vector.append((sys.intern('right_all'), math.sqrt(1 + correct_attempts_all)))
			feature_vector.append((sys.intern('wrong_all'), math.sqrt(1 + total_attempts_all - correct_attempts_all)))
			feature_vector.append((sys.intern('right_session'), math.sqrt(1 + correct_attempts)))
			feature_vector.append((sys.intern('wrong_session'), math.sqrt(1 + total_attempts - correct_attempts)))
			#feature_vector.append((sys.intern(userid), 1.))
			feature_vector.append((sys.intern(examid), 1.))
			feature_vector.append((sys.intern(str(entityid)), 1.))
			inst = Instance(userid, actual_recall, actual_halflife, time_delta, entityid, prev_session_end, feature_vector)
			instances.append(inst)

	splitpoint = int(0.9 * len(instances)) if is_training_phase else 0
	trainset = instances[:splitpoint]
	testset = instances[splitpoint:]
	print ("Total generated instances {}".format(len(instances)))
	return trainset, testset


class HLRModel(object):
	# Default lrate=.001, hlwt=.01, l2wt=.1, sigma=1.
	# Optimized lrate=0.022427189896994885, hlwt=0.0076612918283761695, l2wt=0.18511718781821973, sigma=0.8733141261582174
	def __init__(self, initial_weights=None, lrate=.001, hlwt=.01, l2wt=.1, sigma=1.): 
		self.weights = defaultdict(float)
		if initial_weights is not None:
			self.weights.update(initial_weights)
		self.fcounts = defaultdict(int)
		self.lrate = lrate
		self.hlwt = hlwt
		self.l2wt = l2wt
		self.sigma = sigma
		self.min_loss = float("inf") 

		# use this when running multiple workers, combine once all workers finish
		self.parallel_weights = defaultdict(lambda: defaultdict(float))
		self.parallel_fcounts = defaultdict(lambda: defaultdict(int))


	def halflife(self, inst, base, worker=None):
		# h = 2 ** (theta . x)
		if worker is None:
			theta_x_dot_product = sum([self.weights[feature] * value for (feature, value) in inst.feature_vector])
		else:
			theta_x_dot_product = sum([self.parallel_weights[worker][feature] * value for (feature, value) in inst.feature_vector])
		return halflife_clip(base ** theta_x_dot_product) 


	def predict(self, inst, base=2., worker=None):
		halflife = self.halflife(inst, base, worker)
		recall = get_recall(halflife, inst.time_delta)
		return recall_clip(recall), halflife


	def get_total_loss(self, dataset):
		total_loss = 0.0
		for inst in dataset:
			slr, slh, recall, hl = self.losses(inst)
			total_loss += slr + slh
		return total_loss


	def train_update_sequential(self, inst):
		base = 2.
		recall, hl = self.predict(inst, base)
		dl_recall_dw = 2. * (recall - inst.recall) * (LN2 ** 2) * recall * (inst.time_delta / hl)
		dl_hl_dw = 2. * (hl - inst.hl) * LN2 * hl
		for (feature, value) in inst.feature_vector:
			rate = (1. / (1 + inst.recall)) * self.lrate / math.sqrt(1 + self.fcounts[feature])
			weight_update = self.weights[feature] - rate * dl_recall_dw * value
			weight_update -= rate * self.hlwt * dl_hl_dw * value
			# L2 regularization update
			weight_update -= rate * self.l2wt * self.weights[feature] / self.sigma ** 2
			self.weights[feature] = MIN_WEIGHT if math.isnan(weight_update) else weight_update
			# increment feature count for learning rate
			self.fcounts[feature] += 1

	
	def train_sequential(self, params, trainset, epochs, save_weights):
		if params:
			self.lrate = params['lrate']
			self.hlwt = params['hlwt']
			self.l2wt = params['l2wt']
			self.sigma = params['sigma']

		train_loss = float('inf')

		for i in range(epochs):
			for inst in trainset:
				self.train_update_sequential(inst)
			with open(save_weights, 'w') as f:
				print("\n\nEpoch {}: train_loss {}\n".format(i, self.min_loss))
				f.write(json.dumps(self.weights))
			print ("Weight after epoch {}: {}".format(i, self.weights))
		return {'loss': train_loss, 'status': STATUS_OK, 'params': params}


	def get_batches(self, dataset, size=1024):
		for i in range(0, len(dataset), size):
			yield dataset[i : i + size]		


	"""

	# parallel batch training
	def train_update_parallel(self, worker, queue, trainset):
		while True:
			batch = queue.get()
			for inst in batch:
				base = 2.
				recall, hl = self.predict(inst, base, worker=worker)
				dl_recall_dw = 2. * (recall - inst.recall) * (LN2 ** 2) * recall * (inst.time_delta / hl)
				dl_hl_dw = 2. * (hl - inst.hl) * LN2 * hl
				for (feature, value) in inst.feature_vector:
					rate = (1. / (1 + inst.recall)) * self.lrate / math.sqrt(1 + self.parallel_fcounts[worker][feature])
					weight_update = self.parallel_weights[worker][feature] - rate * dl_recall_dw * value
					weight_update -= rate * self.hlwt * dl_hl_dw * value
					# L2 regularization update
					weight_update -= rate * self.l2wt * self.parallel_weights[worker][feature] / self.sigma ** 2
					self.parallel_weights[worker][feature] = MIN_WEIGHT if math.isnan(weight_update) else weight_update
					# increment feature count for learning rate
					self.parallel_fcounts[worker][feature] += 1
			queue.task_done()
		return self.min_loss 


	def train_parallel(self, params, trainset, epochs, save_weights):
		if params:
			self.lrate = params['lrate']
			self.hlwt = params['hlwt']
			self.l2wt = params['l2wt']
			self.sigma = params['sigma']

		train_loss = float('inf')

		for i in range(epochs):
			for batch in self.get_batches(trainset):
				queue.put(batch)	
			print ("Total batches", queue.qsize())
			for w in range(WORKERS):
				worker = Thread(target=self.train_update_parallel, args=(w, queue, trainset))
				worker.setDaemon(True)
				worker.start() 
			with open(save_weights, 'w') as f:
				print("\n\nEpoch {}: train_loss {}\n".format(i, self.min_loss))
				
				# combining parallel weights
				for w in range(WORKERS):
					for entityid, weight in self.parallel_weights[w].items():
						self.weights[entityid] += weight 
				for entityid, weight in self.weights.items():
					self.weights[entityid] = weight / WORKERS
				self.weights[0] = 1.0 # Hard coding weights for subjects and topics not present in training data (they will have 0 as id because we're using defaultdict(int) for category-chapter map and chapter-subject map
				f.write(json.dumps(self.weights))
			print ("Weight after epoch {}: {}".format(i, self.weights))
		return {'loss': train_loss, 'status': STATUS_OK, 'params': params}

	"""


	# parallel batch training
	def train_update_parallel(self, worker, queue, trainset):
		while True:
			batch = queue.get()
			for inst in batch:
				self.train_update_sequential(inst)
			queue.task_done()
		return self.min_loss 


	def train_parallel(self, params, trainset, epochs, save_weights):
		if params:
			self.lrate = params['lrate']
			self.hlwt = params['hlwt']
			self.l2wt = params['l2wt']
			self.sigma = params['sigma']

		train_loss = float('inf')

		for i in range(epochs):
			for batch in self.get_batches(trainset):
				queue.put(batch)	
			print ("Total batches", queue.qsize())
			for w in range(WORKERS):
				worker = Thread(target=self.train_update_parallel, args=(w, queue, trainset))
				worker.setDaemon(True)
				worker.start() 
			with open(save_weights, 'w') as f:
				print("\n\nEpoch {}: train_loss {}\n".format(i, self.min_loss))
				
				self.weights[0] = 1.0 # Hard coding weights for subjects and topics not present in training data (they will have 0 as id because we're using defaultdict(int) for category-chapter map and chapter-subject map
				f.write(json.dumps(self.weights))
			print ("Weight after epoch {}: {}".format(i, self.weights))
		return {'loss': train_loss, 'status': STATUS_OK, 'params': params}


	def train_with_param_optimization(self, trainset, epochs, save_weights, param_opt_rounds):
		bayes_trials = Trials()
		objective_fn = partial(self.train_parallel, trainset=trainset, epochs=epochs, save_weights=save_weights)
		best_params = fmin(fn = objective_fn, space = hyper_param_space, algo = tpe.suggest, max_evals = param_opt_rounds, trials = bayes_trials)		
		bayes_trials_results = sorted(bayes_trials.results, key = lambda x: x['loss'])
		print ("\nTop trials:\n{}".format(bayes_trials_results[:2]))
		print ("\nBest Params:\n{}\n".format(best_params))

	
	def losses(self, inst):
		recall, hl = self.predict(inst)
		stop_loss_recall = (inst.recall - recall) ** 2
		stop_loss_hl = (inst.hl - hl) ** 2
		return stop_loss_recall, stop_loss_hl, recall, hl


	def eval(self, testset):
		print ("Predicting...")
		results = {'recall': [], 'hl': [], 'pred_recall': [], 'pred_hl': [], 'sl_recall': [], 'sl_hl': []}
		recalls = [inst.recall for inst in testset]
		hls = [inst.hl for inst in testset]
		time_deltas = [inst.time_delta for inst in testset]
		halflives_in_practice = list()
		for inst in testset:
			sl_recall, sl_hl, recall, hl = self.losses(inst)
			results['recall'].append(inst.recall)	 # ground truth
			results['hl'].append(inst.hl)
			results['pred_recall'].append(recall)		 # predictions
			results['pred_hl'].append(hl)
			results['sl_recall'].append(sl_recall)	  # loss function values
			results['sl_hl'].append(sl_hl)
			hl_in_practice = get_hl_in_practice(inst.recall, hl) 
			halflives_in_practice.append(hl_in_practice)
			print ("\n\nrecall a:p {}:{}, hl  a:p {}:{}, hl_in_practice {} days, lag {}\nfeature_vec {}".format(inst.recall, recall, inst.hl, hl, hl_in_practice, inst.time_delta, inst.feature_vector))
		graph_df = pd.DataFrame(list(zip(halflives_in_practice, recalls)), columns=['H', 'R'])
		plot = sb.lmplot(x='R', y='H', data = graph_df)
		plot.savefig('graph.png')
		mae_recall = mae(results['recall'], results['pred_recall'])
		mae_hl = mae(results['hl'], results['pred_hl'])
		cor_recall = spearmanr(results['recall'], results['pred_recall'])
		cor_hl = spearmanr(results['hl'], results['pred_hl'])
		total_sl_recall = sum(results['sl_recall'])
		total_sl_hl = sum(results['sl_hl'])
		total_l2 = sum([x ** 2 for x in self.weights.values()])
		total_loss = total_sl_recall + self.hlwt * total_sl_hl + self.l2wt * total_l2
		print('\ntotal_loss=%.1f (recall=%.1f, hl=%.1f, l2=%.1f)\tmae(recall)=%.3f\tcor(recall)=%.3f\tmae(hl)=%.3f\tcor(hl)=%.3f\n' % (total_loss, total_sl_recall, self.hlwt * total_sl_hl, self.l2wt * total_l2, mae_recall, cor_recall, mae_hl, cor_hl))


def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument('--pretrained-weights', dest='pretrained_weights', help="JSON file containing trained weights")
	parser.add_argument('--save-weights-path', dest='save_weights_path', default="", help='File to save the weights in')
	parser.add_argument('--epochs', dest='epochs', type=int, default=1, help='Epochs to train')
	parser.add_argument('--param-opt-rounds', dest='param_opt_rounds', type=int, default=0, help='Hyperparameter optimization rounds')
	parser.add_argument('--train', dest='train', default=True, action="store_true", help='Train the model')
	parser.add_argument('--pred', dest='pred', default=False, action="store_true", help='Predict halflife')
	parser.add_argument('--subject-as-entity', dest='subject_as_entity', default=False, action="store_true", help='Train/predict at subject level, instead of chapters')
	parser.add_argument('--optimize-params', dest='optimize_params', default=False, action="store_true", help='Optimize hyperparameters')
	parser.add_argument('--convert-to-chapters', dest='convert_to_chapters', default=False, action="store_true", help='Convert categoryid to chapters. Use when csv contains categoryid instead of chapterid')
	parser.add_argument('attempts_folder', help='Folder containing attempts data in CSV format')
	args = parser.parse_args()
	if not args.attempts_folder:
		sys.exit('Please pass the attempts data folder')
	return args


def get_final_df(df, convert_to_chapters, working_at_subject_level=False):
	df['date'] = df.apply(lambda row: dt.fromtimestamp(row['attempttime']/1000).date(), axis=1) 
	if convert_to_chapters:
		df['chapterid'] = df.apply(lambda row: category_to_chapter_map[str(int(row['categoryid']))], axis=1)
		df.drop(columns=['categoryid'])
	if working_at_subject_level:
		df['subjectid'] = df.apply(lambda row: chapter_to_subject_map[str(int(row['chapterid']))], axis=1)
	df.dropna()
	df = df.sort_values(by=['attempttime'], ascending=True)
	return df	


def get_model(pretrained_weights_path, mode='one_time_use'):
	pretrained_weights = json.load(open(pretrained_weights_path, 'r')) if pretrained_weights_path else dict() 
	if mode == 'one_time_use':
		return HLRModel(initial_weights=pretrained_weights)
	else:
		global inference_model
		if inference_model is None:
			inference_model = HLRModel(initial_weights=pretrained_weights)
		return inference_model


# Only method that is to be called from other classes for prediction on raw_df
def run_inference(raw_df, entity_type, last_practiced_map, convert_to_chapters=True):
	working_at_subject_level = entity_type == 'subject'
	df = get_final_df(raw_df, convert_to_chapters, working_at_subject_level=working_at_subject_level)
	model = get_model(PRETRAINED_WEIGHTS[entity_type], mode='multi_use')
	_, instances = get_instances(df, False, working_at_subject_level=working_at_subject_level, last_practiced_map=last_practiced_map)
	results = list()
	for inst in instances:
		_, hl = model.predict(inst)
		hl_in_practice = get_hl_in_practice(inst.recall, hl) 
		results.append(Result(inst.userid, inst.entityid, inst.last_practiced_at, inst.recall, hl_in_practice))
	return results


if __name__=='__main__':
	args = parse_args()
	is_training_phase = args.train and not args.pred 

	if is_training_phase and not args.save_weights_path:
		print ("Provide a path to save trained weights. Stopping..")
		sys.exit(0)	

	use_pretrained_weights = True
	for attempts_file in os.listdir(args.attempts_folder):

		print ("Current file:", attempts_file)
		df = get_final_df(pd.read_csv(os.path.join(args.attempts_folder, attempts_file)), args.convert_to_chapters, working_at_subject_level=args.subject_as_entity)

		queue = Queue()
		
		model = get_model(args.pretrained_weights if use_pretrained_weights else args.save_weights_path)
		use_pretrained_weights = args.pred # If running prediction, keep on using pretrained weights, otherwise use the updated weights

		trainset, testset = get_instances(df, is_training_phase, working_at_subject_level=args.subject_as_entity)
		print ("trainset {}, testset {}".format(len(trainset), len(testset)))

		if is_training_phase:	
			if args.optimize_params or args.param_opt_rounds > 0:
				model.train_with_param_optimization(trainset, args.epochs, args.save_weights_path, HYPER_PARAM_OPT_ROUNDS if args.param_opt_rounds == 0 else args.param_opt_rounds)
			else:
				model.train_parallel(None, trainset, args.epochs, args.save_weights_path)
		else:
			model.eval(testset)
		queue.join()
