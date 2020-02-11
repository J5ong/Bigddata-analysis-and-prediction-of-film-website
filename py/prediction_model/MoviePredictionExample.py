import sys
import numpy as np
import tensorflow as tf

#数据格式：[[上座率1,排座占比1,上座率2,排座占比2,上座率3,排座占比3,上座率4,排座占比4,上座率5,排座占比5,上座率6,排座占比6,上座率7,排座占比7,贺岁档,五一档,暑假档,国庆档,其他档,中国,美国,其他,]]
def movie_prediction(args):
	# data=tf.constant([[0.067000,0.164000,0.098000,0.135000,0.084000,0.123000,0.040000,0.123000,0.038000,0.117000,0.035000,0.115000,0.031000,0.110000,0.000000,0.000000,0.000000,0.000000,1.000000,0.000000,1.000000,0.000000]])
	data=tf.constant([[args[0],args[1],args[2],args[3],args[4],args[5],args[6],args[7],args[8],args[9],args[10],args[11],args[12],args[13],args[14],args[15],args[16],args[17],args[18],args[19],args[20],args[21]]])


	sess=tf.Session()
	sess.run(tf.global_variables_initializer())

	W0=np.float32(np.loadtxt('./prediction_model/W0.csv',delimiter=','))
	W1=np.float32(np.loadtxt('./prediction_model/W1.csv',delimiter=','))
	W2=np.float32(np.expand_dims(np.loadtxt('./prediction_model/W2.csv',delimiter=','),axis=1))
	b0=np.float32(np.loadtxt('./prediction_model/b0.csv',delimiter=','))
	b1=np.float32(np.loadtxt('./prediction_model/b1.csv',delimiter=','))
	b2=np.float32(np.loadtxt('./prediction_model/b2.csv',delimiter=','))


	hide_layer1=tf.nn.relu(tf.matmul(data, W0) + b0)
	hide_layer2=tf.nn.sigmoid(tf.matmul(hide_layer1,W1)+b1)
	output_layer=tf.matmul(hide_layer2,W2)+b2

	return str(round(sess.run(output_layer).tolist()[0][0],2))