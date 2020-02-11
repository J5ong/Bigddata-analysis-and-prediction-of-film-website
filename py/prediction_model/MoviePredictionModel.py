import numpy as np
import tensorflow as tf
import pandas as pd

#数据处理
def getTransition(i,m):
    list=[0 for n in range(m)]
    k=list.__len__()
    for j in range(i.__len__()-1, -1, -1):
        if(k-1>=0):
            list[k - 1] = i[j]
            k = k - 1
    return list

df=pd.DataFrame(pd.read_csv('output_yr_new.csv'))

#电影id
id_matrix=np.array(df.iloc[0:df.__len__(),0:1])

#档期
schedule_matrix=np.zeros((df.__len__(),5))
for i in range(df.__len__()):
    schedule_list=getTransition(str(df['档期'][i]),5)
    for j in range(5):
        schedule_matrix[i][j]=schedule_list[j]
# print(schedule_matrix)

#制片地区
area_matrix=np.zeros((df.__len__(),3))
for i in range(df.__len__()):
    area_list=getTransition(str(df['制片地区'][i]),3)
    for j in range(3):
        area_matrix[i][j]=area_list[j]
# print(area_matrix)

#类型
type_matrix=np.zeros((df.__len__(),22))
for i in range(df.__len__()):
    type_list=getTransition(df['类型'][i],22)
    for j in range(22):
        type_matrix[i][j]=type_list[j]
# print(type_matrix)

#7天的上座率和占座比
seven_days_matrix=np.array(df.iloc[0:df.__len__(),3:17])
# print(seven_days_matrix)

# print(data)

#冰点天数 结果集
reult_list=np.array(df['结果天数'])
reult_matrix=reult_list.reshape((reult_list.__len__(),1))

#数据集
data=np.hstack((id_matrix,seven_days_matrix,
                # type_matrix,
                schedule_matrix,
                area_matrix,
                reult_matrix))


data_input=data[:,1:23]
data_target=data[:,23:]
index_train = [i for i in range(1000)]
index_test = [i for i in range(1000,data.__len__())]

#训练集部分
x_data_train=np.float32(data_input[index_train, :])
y_data_train=np.float32(data_target[index_train, :])
#测试集部分
x_data_test=np.float32(data_input[index_test,:])
y_data_test=np.float32(data_target[index_test,:])
#数据集整体
x_data_all=np.float32(data_input)
y_data_all=np.float32(data_target)

#隐藏层和输出层节点个数设计
W0=tf.Variable(tf.random_normal([22,64]))
b0=tf.Variable(tf.zeros([64])+0.01)
W1=tf.Variable(tf.random_normal([64,128]))
b1=tf.Variable(tf.zeros([128])+0.01)
W2=tf.Variable(tf.random_normal([128,1]))
b2=tf.Variable(tf.zeros([1])+0.01)

hide_layer1=tf.nn.relu(tf.matmul(x_data_train, W0) + b0)#隐藏层1
hide_layer2=tf.nn.sigmoid(tf.matmul(hide_layer1,W1)+b1)#隐藏层2
output_layer=tf.matmul(hide_layer2,W2)+b2#输出层

loss = tf.reduce_mean(tf.reduce_sum(tf.square(y_data_train - output_layer),
                                    reduction_indices=[1]))#计算均方误差
train_step=tf.train.AdamOptimizer(0.001).minimize(loss)#设计训练梯度

sess=tf.Session()
sess.run(tf.global_variables_initializer())#初始化tensorflow参数


for i in range(2000):#训练开始
    sess.run(train_step)
    if i%50==0:
        print("训练集预测MSE：",sess.run(loss))
        hide_layer_test1 = tf.nn.relu(tf.matmul(x_data_test, W0) + b0)
        hide_layer_test2 = tf.nn.sigmoid(tf.matmul(hide_layer_test1, W1) + b1)
        output_layer_test = tf.matmul(hide_layer_test2, W2) + b2
        print("测试集预测值与真实值的平均差值",sum(np.array(abs(y_data_test - sess.run(output_layer_test)))) / index_test.__len__())

out_train=np.hstack((y_data_train,sess.run(output_layer)))
np.savetxt('out_train.csv',out_train,fmt='%.2f',delimiter=',')

np.savetxt('W0.csv',sess.run(W0),delimiter=',')
np.savetxt('W1.csv',sess.run(W1),delimiter=',')
np.savetxt('W2.csv',sess.run(W2),delimiter=',')
np.savetxt('b0.csv',sess.run(b0),delimiter=',')
np.savetxt('b1.csv',sess.run(b1),delimiter=',')
np.savetxt('b2.csv',sess.run(b2),delimiter=',')

out_test=np.hstack((y_data_test,sess.run(output_layer_test),sess.run(output_layer_test)-y_data_test))
np.savetxt('out_test.csv',out_test,fmt='%.2f',delimiter=',')
# print(sum(np.array(abs(y_data_test-sess.run(output_layer_test))))/index_test.__len__())

hide_layer1_all=tf.nn.relu(tf.matmul(x_data_all,W0)+b0)
hide_layer2_all=tf.nn.sigmoid(tf.matmul(hide_layer1_all,W1)+b1)
output_layer_all=tf.matmul(hide_layer2_all,W2)+b2
out_all=np.hstack((sess.run(output_layer_all),y_data_all))
np.savetxt('out_all.csv',out_all,fmt='%.2f',delimiter=',')
print("数据集预测值与真实值的平均差值",sum(np.array(abs(y_data_all-sess.run(output_layer_all))))/data.__len__())