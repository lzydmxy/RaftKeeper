#include <DB/AggregateFunctions/UniqCombinedBiasData.h>

namespace DB
{

double UniqCombinedBiasData::getThreshold()
{
	return 176000;
}

const UniqCombinedBiasData::InterpolatedData & UniqCombinedBiasData::getRawEstimates()
{
	static const InterpolatedData values =
	{
		700.0
		,3850.0
		,7350.0
		,10850.0
		,14350.0
		,89003.5714
		,103764.30343333333
		,105572.1915
		,109252.46533333334
		,112638.20573333332
		,116094.29566666669
		,119619.81926666666
		,123214.92233333334
		,126469.06656666666
		,130196.15093333334
		,133566.85673333335
		,136991.63890000002
		,140470.0118666667
		,144000.91686666667
		,147585.44463333333
		,151222.7466
		,154447.75893333333
		,158181.68399999998
		,161492.41386666667
		,164840.6352
		,168713.9904
		,172143.82656666666
		,175611.2078
		,179116.94873333335
		,182658.0355
		,186236.36723333332
		,189332.1009
		,192976.1847
		,196654.62706666664
		,199835.39103333335
		,203575.92429999998
		,206808.87086666666
		,210611.72886666664
		,213896.25913333334
		,217759.63066666664
		,221096.10933333333
		,224456.31466666667
		,227839.0366333333
		,231242.72576666667
		,235239.98256666667
		,238688.95070000002
		,242158.17593333332
		,245649.42926666664
		,249158.9859666667
		,252689.67179999998
		,256241.95376666667
		,259214.9391666667
		,262798.3925666667
		,266399.8345666667
		,270018.35863333335
		,273653.1149
		,276696.7119
		,280366.51476666663
		,284051.95540000004
		,287133.5254333333
		,290847.31173333334
		,294579.5226
		,297698.64109999995
		,301454.39253333333
		,305223.59123333334
		,308375.3184666667
		,312170.06
		,315342.02996666665
		,319162.8188666667
		,322356.3565666666
		,326199.5866
		,329412.83396666666
		,332634.3235666667
		,336510.7596333333
		,339747.7330333333
		,343643.0385666667
		,346896.77420000004
		,350157.6729666667
		,354079.3932333334
		,357354.5196333334
		,360638.3034333333
		,364588.47873333335
		,367886.05706666666
		,371189.98006666667
		,375161.95876666665
		,378478.6737666666
		,381801.6619
		,385130.9645
		,389131.7460333333
		,392471.6233333333
		,395817.1175
		,399165.1003333333
		,402518.7819333333
		,406549.7624333333
		,409916.016
		,413289.0218666666
		,416661.9977333333
		,420040.4257333334
		,424099.3186333333
		,427485.4292000001
		,430876.4814666666
		,434269.4718
		,437665.82826666674
		,441066.7185
		,444469.97226666665
		,448561.9376666667
		,451974.73750000005
		,455389.1112
		,458808.5816666667
		,462230.8184666667
		,465656.9889
		,469081.3269
		,472512.4878
		,475944.4204333333
		,480065.7132666667
		,483502.04110000003
		,486939.5075666667
		,490379.7868333334
		,493818.5365333333
		,497259.08013333334
		,500705.3513
		,504155.6234666666
		,507606.65499999997
		,511060.7448666667
		,514517.4004
		,517973.35829999996
		,521431.3761666666
		,524891.7097333333
		,529044.7593
		,532507.0878999999
		,535971.5070333333
		,539436.2416999999
		,542903.1470333333
		,546370.3423
		,549837.6947999999
		,553307.0003666667
		,556775.3770333333
		,560247.6308333334
		,563721.0700333334
		,567196.7586333333
		,570669.8439666666
		,574146.018
		,577622.2794666667
		,581098.3862333334
		,584575.8826666666
		,588055.1468000001
		,591538.0234
		,595018.0103000001
		,598504.5469333333
		,601992.5697666666
		,605475.5452
		,608959.4645
		,612444.0261
		,615929.6436
		,619412.3877333334
		,622903.4263999999
		,626391.3657333333
		,629876.7359333333
		,633364.2825999999
		,636855.2673666667
		,640344.4321000001
		,643836.5543666667
		,647327.3073999999
		,650818.3525666667
		,654312.2421666667
		,657807.0899666668
		,661301.4443666666
		,664794.1040333334
		,668288.1969666666
		,671781.0196666667
		,675272.7522333333
		,678766.9045999999
		,682259.3583666667
		,685747.8148333334
		,689238.7994666666
		,692732.0478333334
		,696224.6407
		,700069.9224
	};

	return values;
}

const UniqCombinedBiasData::InterpolatedData & UniqCombinedBiasData::getBiases()
{
	static const InterpolatedData values =
	{
		0.0
		,0.0
		,0.0
		,0.0
		,0.0
		,71153.5714
		,85214.30343333333
		,83522.1915
		,80202.46533333334
		,77288.20573333332
		,74444.29566666667
		,71669.81926666667
		,68964.92233333334
		,66619.06656666666
		,64046.15093333333
		,61816.85673333333
		,59641.6389
		,57520.01186666667
		,55450.91686666667
		,53435.44463333334
		,51472.74659999999
		,49797.75893333333
		,47931.68399999999
		,46342.41386666667
		,44790.6352
		,43063.9904
		,41593.82656666667
		,40161.2078
		,38766.94873333333
		,37408.035500000005
		,36086.36723333333
		,34982.1009
		,33726.184700000005
		,32504.627066666664
		,31485.391033333333
		,30325.924299999995
		,29358.870866666668
		,28261.72886666667
		,27346.259133333337
		,26309.630666666668
		,25446.109333333337
		,24606.31466666666
		,23789.036633333333
		,22992.725766666666
		,22089.98256666667
		,21338.9507
		,20608.175933333332
		,19899.429266666673
		,19208.985966666663
		,18539.6718
		,17891.95376666667
		,17364.939166666667
		,16748.392566666666
		,16149.834566666666
		,15568.358633333331
		,15003.114899999995
		,14546.711900000004
		,14016.51476666668
		,13501.955399999997
		,13083.52543333332
		,12597.311733333336
		,12129.522600000006
		,11748.641100000008
		,11304.392533333332
		,10873.59123333334
		,10525.318466666678
		,10120.059999999998
		,9792.029966666674
		,9412.818866666668
		,9106.356566666664
		,8749.58660000001
		,8462.833966666678
		,8184.323566666659
		,7860.759633333325
		,7597.733033333323
		,7293.038566666665
		,7046.774200000004
		,6807.672966666675
		,6529.393233333336
		,6304.519633333344
		,6088.30343333332
		,5838.4787333333325
		,5636.057066666661
		,5439.980066666671
		,5211.958766666658
		,5028.673766666664
		,4851.661899999996
		,4680.964499999992
		,4481.746033333319
		,4321.623333333322
		,4167.117500000012
		,4015.1003333333356
		,3868.781933333337
		,3699.762433333332
		,3566.0159999999937
		,3439.021866666648
		,3311.9977333333422
		,3190.4257333333276
		,3049.3186333333238
		,2935.4291999999937
		,2826.4814666666593
		,2719.4717999999993
		,2615.8282666666782
		,2516.7184999999977
		,2419.972266666669
		,2311.9376666666744
		,2224.7374999999884
		,2139.1111999999944
		,2058.581666666665
		,1980.8184666666687
		,1906.9888999999966
		,1831.3268999999952
		,1762.4878000000026
		,1694.420433333328
		,1615.7132666666682
		,1552.0410999999924
		,1489.507566666677
		,1429.7868333333365
		,1368.536533333332
		,1309.0801333333268
		,1255.35129999999
		,1205.6234666666617
		,1156.6549999999988
		,1110.744866666675
		,1067.4004000000034
		,1023.3583000000023
		,981.3761666666638
		,941.7097333333513
		,894.7593000000148
		,857.0879000000035
		,821.5070333333375
		,786.2416999999745
		,753.1470333333127
		,720.3422999999797
		,687.6947999999975
		,657.0003666666647
		,625.3770333333329
		,597.6308333333387
		,571.0700333333225
		,546.7586333333165
		,519.8439666666478
		,496.0180000000012
		,472.2794666666693
		,448.386233333343
		,425.8826666666816
		,405.1468000000071
		,388.0233999999861
		,368.01030000002356
		,354.54693333333125
		,342.5697666666626
		,325.5452000000126
		,309.4644999999825
		,294.0261000000173
		,279.64360000001034
		,262.38773333333666
		,253.42639999999665
		,241.36573333333945
		,226.7359333333443
		,214.28259999999622
		,205.26736666667662
		,194.43210000001514
		,186.55436666666841
		,177.30740000001
		,168.35256666666828
		,162.24216666668266
		,157.0899666666713
		,151.44436666666297
		,144.1040333333464
		,138.19696666668946
		,131.01966666666945
		,122.7522333333424
		,116.90459999998954
		,109.35836666667213
		,97.81483333332774
		,88.7994666666491
		,82.04783333333519
		,74.64070000000841
		,69.92240000003949
	};

	return values;
}

}
