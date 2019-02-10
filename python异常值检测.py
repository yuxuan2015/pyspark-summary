python异常值检测

df_outliers = DataFrame(np.array([
[1, 143.5, 5.3, 28],
[2, 154.2, 5.5, 45],
[3, 342.3, 5.1, 99],
[4, 144.5, 5.5, 33],
[5, 133.2, 5.4, 54],
[6, 124.1, 5.1, 21],
[7, 129.2, 5.3, 42],
]), columns=['id', 'weight', 'height', 'age'])

1. 一元outliers检测
	1.1 四分位数法
	quantiles_25 = df_outliers.quantile(.25)
	quantiles_75 = df_outliers.quantile(.75)
	IQR = quantiles_75 - quantiles_25
	outliers = ((df_outliers < quantiles_25 - 1.5 * IQR) | (df_outliers > quantiles_75 + 1.5 * IQR))
	means = df_outliers.mean()
	#用均值替换outliers
	df_outliers = df_outliers.mask(outliers, means, axis=1)
	df_outliers = df_outliers.where(~outliers, means, axis=1)
	
	1.2 3\sigma原则(正态分布)
	means = df_outliers.mean()
	stds = df_outliers.std()
	outliers = ((df_outliers < means - 3 * stds) | (df_outliers > means + 3 * stds))
	
2. 多元outliers检测
	2.1 基于一元正态分布的outliers检测
	mean_std = {}
	for value in df_outliers.columns:
		describes = df_outliers[value].describe().tolist()
		mean_std[value] = [describes[1], describes[2]]

	def outliers_p(value):
		values = value.split(',')
		for i in len(values):
			
3. 基于机器学习算法
	3.1 用IsolationForest做异常检测
	http://scikit-learn.org/stable/modules/generated/sklearn.ensemble.IsolationForest.html
	sklearn.ensemble.IsolationForest(n_estimators=100, max_samples='auto', contamination=0.1, max_features=1.0, bootstrap=False, n_jobs=1, random_state=None, verbose=0)[source]
	contamination控制异常值的比例

	from sklearn.ensemble import IsolationForest

	# fit the model
	clf = IsolationForest(max_samples=100, random_state=rng, n_jobs=-1)
	clf.fit(X_train)
	y_pred_train = clf.predict(X_train)
	y_pred_test = clf.predict(X_test)
	y_pred_outliers = clf.predict(X_outliers)
	
	3.2 OneClassSVM
	http://scikit-learn.org/stable/modules/generated/sklearn.svm.OneClassSVM.html
	sklearn.svm.OneClassSVM(kernel='rbf', degree=3, gamma='auto', coef0=0.0, tol=0.001, nu=0.5, shrinking=True, cache_size=200, verbose=False, max_iter=-1, random_state=None)
	
	from sklearn.svm import OneClassSVM
	
	ocm = OneClassSVM(kernel='rbf', nu=0.5, gamma='auto')
	ocm.fit(features)
	predict = ocm.predict(features)
	
	3.3 EllipticEnvelope
	http://scikit-learn.org/stable/modules/generated/sklearn.covariance.EllipticEnvelope.html#sklearn.covariance.EllipticEnvelope
	sklearn.covariance.EllipticEnvelope(store_precision=True, assume_centered=False, support_fraction=None, contamination=0.1, random_state=None)
	
	from sklearn.covariance import EllipticEnvelope