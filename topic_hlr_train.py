# -*- coding: utf-8 -*-
"""topic-hlr-train.ipynb

Automatically generated by Colaboratory.

Original file is located at
	https://colab.research.google.com/drive/1P6QsFOhU8pusyMXomLsjCkfyQW7exD5H
"""

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

MIN_REC = 0.0001
MAX_REC = 0.9999
MIN_HL = 0.0001 #min
MAX_HL = 180.0 #min
MIN_WEIGHT = 0.0001
LN2 = math.log(2.)

Instance = namedtuple('Instance', ['recall', 'hl', 'time_delta', 'feature_vector'])

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
	return float(sum(lst)) / len(lst)


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


def read_data(df):
	print ("Reading data...")
	instances = list()
	has_practiced_before = 0
	#for groupid, groupdf in df.groupby(['userid', 'examid', 'categoryid']):
	for groupid, groupdf in df.groupby(['examid']):
		print ("exam_group", groupid, len(groupdf))
		prev_session_end = None
		#userid = groupid[0]
		userid = 'u0'
		#examid = groupid[1]
		examid = groupid
		#categoryid = groupid[2]
		categoryid = 1
		correct_attempts_all = 0
		total_attempts_all = 0
		#for sessionid, session_group in groupdf.groupby(['date']):
		for sessionid, session_group in groupdf.groupby(['date', 'hour', 'min']):
			session_name = "{}".format(sessionid)
			print ("minute_group", session_name, len(session_group))
			total_attempts = len(session_group)
			total_attempts_all += total_attempts
			correct_df = session_group[session_group['iscorrect'] == True]
			correct_attempts = len(correct_df)
			correct_attempts_all += correct_attempts
			actual_recall = recall_clip(float(correct_df['difficulty'].sum()) / session_group['difficulty'].sum())
			session_begin_time = session_group['attempttime'].min()
			#time_delta = float('inf') if not prev_session_end else to_days(session_begin_time - prev_session_end)
			time_delta = float('inf') if not prev_session_end else to_minutes(session_begin_time - prev_session_end)
			if time_delta != float('inf'):
				has_practiced_before += 1 
			actual_halflife = MAX_HL if time_delta == float('inf') else halflife_clip(-time_delta / math.log(actual_recall, 2))
			prev_session_end = session_group['attempttime'].max()

			feature_vector = list()
			feature_vector.append((sys.intern('right_all'), math.sqrt(1 + correct_attempts_all)))
			feature_vector.append((sys.intern('wrong_all'), math.sqrt(1 + total_attempts_all - correct_attempts_all)))
			feature_vector.append((sys.intern('right_session'), math.sqrt(1 + correct_attempts)))
			feature_vector.append((sys.intern('wrong_session'), math.sqrt(1 + total_attempts - correct_attempts)))
			feature_vector.append((sys.intern(userid), 1.))
			feature_vector.append((sys.intern(examid), 1.))
			feature_vector.append((sys.intern(str(categoryid)), 1.))
			inst = Instance(actual_recall, actual_halflife, time_delta, feature_vector)
			instances.append(inst)

			print ("Data Instance: ", inst)
		print ("has_practiced_before", has_practiced_before)

	splitpoint = int(0.9 * len(instances))
	trainset = instances[:splitpoint]
	testset = instances[splitpoint:]
	return trainset, testset


class HLRModel(object):
	# Default lrate=.001, hlwt=.01, l2wt=.1, sigma=1.
	def __init__(self, initial_weights=None, lrate=.001, hlwt=.01, l2wt=.1, sigma=1.): 
		self.weights = defaultdict(float)
		self.best_weights = defaultdict(float)
		if initial_weights is not None:
			self.weights.update(initial_weights)
			self.best_weights.update(initial_weights)
		self.fcounts = defaultdict(int)
		self.lrate = lrate
		self.hlwt = hlwt
		self.l2wt = l2wt
		self.sigma = sigma
		self.min_loss = float("inf") 


	def halflife(self, inst, base):
		# h = 2 ** (theta . x)
		theta_x_dot_product = sum([self.weights[feature] * value for (feature, value) in inst.feature_vector])
		print ("halflife_no_clip", base ** theta_x_dot_product)
		return halflife_clip(base ** theta_x_dot_product) 


	def predict(self, inst, base=2.):
		halflife = self.halflife(inst, base)
		recall = 2. ** (-inst.time_delta / halflife)
		return recall_clip(recall), halflife


	def get_total_loss(self, dataset):
		total_loss = 0.0
		for inst in dataset:
			slr, slh, recall, hl = self.losses(inst)
			total_loss += slr + slh
		return total_loss


	def train_update(self, inst, trainset):
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
			print ("\nweights", self.weights)
		train_loss = self.get_total_loss(trainset)			
		if train_loss < self.min_loss:
			self.min_loss = train_loss
			self.best_weights.update(self.weights)
		print ("train_update rec %.2f, hl %.2f, train_loss %.3f (min %.3f)" % (recall, hl, train_loss, self.min_loss))
		return self.min_loss 

	
	def _train(self, params):
		self.lrate = params['lrate']
		self.hlwt = params['hlwt']
		self.l2wt = params['l2wt']
		self.sigma = params['sigma']

		train_loss = float('inf')

		for i in tqdm(range(args.epochs), desc="Epoch "):
			for inst in tqdm(trainset, desc="Training Instance "):
				train_loss = self.train_update(inst, trainset)
			with open(args.save_weights, 'w') as f:
				print("\n\nEpoch {}: train_loss {}\n".format(i, self.min_loss))
				f.write(json.dumps(self.best_weights))
		return {'loss': train_loss, 'status': STATUS_OK, 'params': params}


	def train(self):
		bayes_trials = Trials()
		best_params = fmin(fn = self._train, space = hyper_param_space, algo = tpe.suggest, max_evals = 50, trials = bayes_trials)		
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
		for inst in testset:
			sl_recall, sl_hl, recall, hl = self.losses(inst)
			results['recall'].append(inst.recall)	 # ground truth
			results['hl'].append(inst.hl)
			results['pred_recall'].append(recall)		 # predictions
			results['pred_hl'].append(hl)
			results['sl_recall'].append(sl_recall)	  # loss function values
			results['sl_hl'].append(sl_hl)
			print ("actual_rec {}, pred_rec {}, actual_hl {}, pred_hl {}, sl_rec {}, sl_hl {}".format(inst.recall, recall, inst.hl, hl, sl_recall, sl_hl))
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
	parser.add_argument('--weights', dest='weights', help="JSON file containing trained weights")
	parser.add_argument('--save-weights', dest='save_weights', default="saved_weights.csv", help='File to save the weights in')
	parser.add_argument('--epochs', dest='epochs', type=int, default=1, help='Epochs to train')
	parser.add_argument('--train-further', dest='train_further', default=False, action="store_true", help='If the weights should be trained further')
	parser.add_argument('attempts_file', help='CSV file containing attempts data')
	args = parser.parse_args()
	if not args.attempts_file:
		sys.exit('Please pass the attempts file')
	return args


if __name__=='__main__':
	args = parse_args()
	df = pd.read_csv(args.attempts_file)
	df['date'] = df.apply(lambda row: dt.fromtimestamp(row['attempttime']/1000).date(), axis=1) 
	df['hour'] = df.apply(lambda row: dt.fromtimestamp(row['attempttime']/1000).hour, axis=1)
	df['min'] = df.apply(lambda row: dt.fromtimestamp(row['attempttime']/1000).min, axis=1)
	df = df.sort_values(by=['attempttime'], ascending=True)

	trainset, testset = read_data(df)
	
	if args.weights:
		saved_weights = json.load(open(args.weights), 'r')
		model = HLRModel(initial_weights=saved_weights)
	else:
		saved_weights = None
		model = HLRModel()

	
	if not saved_weights or (saved_weights is not None and args.train_further):
		model.train()

	model.eval(testset)
