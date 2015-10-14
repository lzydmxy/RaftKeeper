#include <DB/AggregateFunctions/UniqCombinedBiasData.h>

namespace DB
{

namespace
{

const UniqCombinedBiasData::InterpolatedData raw_estimates =
{
	99791.8496
	,101386.91930000001
	,105450.95623333333
	,108128.01393333334
	,110851.10286666667
	,113620.01383333335
	,116434.98796666665
	,119295.74893333332
	,122202.58199999998
	,124783.45270000001
	,127775.84493333333
	,130432.03390000002
	,133122.13506666667
	,136239.7482
	,139004.69996666667
	,141803.40813333335
	,144228.62236666665
	,147089.61343333335
	,149984.35636666667
	,152912.8223666667
	,155449.4413666667
	,158440.23733333332
	,161029.4043
	,164080.25746666666
	,166720.31723333334
	,169384.27826666666
	,172521.4491666667
	,175235.4233
	,177971.46556666668
	,180730.04403333334
	,183510.69883333333
	,186313.77773333332
	,189138.67343333332
	,191985.62490000002
	,194853.55733333333
	,197259.4243333333
	,200165.33826666666
	,203093.2792
	,205550.0133666667
	,208515.49296666667
	,211500.27113333336
	,214002.73933333336
	,217022.66503333332
	,219554.61286666666
	,222611.62203333332
	,225172.43516666666
	,228261.63369999998
	,230849.9269333333
	,233450.9665
	,236588.48176666666
	,239217.14506666665
	,241858.01729999995
	,245040.88769999996
	,247707.505
	,250385.32816666667
	,253072.74516666666
	,255772.3767333333
	,259026.62416666665
	,261750.1933
	,264484.4988666667
	,267229.4741
	,269983.9762
	,272747.6032
	,275521.9937
	,278306.35263333336
	,281100.67233333335
	,283902.65756666666
	,286716.28403333336
	,289537.95599999995
	,292368.9353666667
	,295207.7315
	,298055.9653333333
	,300911.9654666667
	,303204.35336666665
	,306077.9537333333
	,308958.00193333335
	,311845.22890000005
	,314741.81600000005
	,317644.8173333333
	,319972.31696666667
	,322888.63776666665
	,325811.89053333335
	,328741.73743333324
	,331091.32163333334
	,334034.29806666664
	,336984.6469666666
	,339939.86216666666
	,342309.7939
	,345278.14656666666
	,348252.3204333333
	,350635.0094666667
	,353618.8034000001
	,356610.7431333333
	,359005.6872333333
	,362005.8481
	,365011.9431333333
	,367422.15616666665
	,370439.9724666667
	,373460.6025
	,375879.31826666667
	,378908.1752
	,381335.98703333334
	,384373.7107666666
	,387416.2068333333
	,389852.7087666667
	,392901.8697
	,395343.33469999995
	,398401.5141333333
	,400851.9174
	,403917.6844666666
	,406371.6598333334
	,409440.80490000005
	,412517.26203333336
	,414981.9741666666
	,418063.8305
	,420530.6776
	,423616.6512666666
	,426088.72699999996
	,429181.1127666666
	,431657.64166666666
	,434757.3337
	,437235.97023333336
	,440338.2023666667
	,442823.12679999997
	,445932.7757666667
	,448419.81309999997
	,451533.39386666665
	,454026.96746666665
	,457147.8259333333
	,459643.8253666666
	,462140.6687333334
	,465264.5323
	,467767.3770333333
	,470899.63109999994
	,473406.5693999999
	,476540.8793333333
	,479051.11850000004
	,482189.9576
	,484701.15849999996
	,487836.66456666664
	,490348.32859999995
	,492863.5349666667
	,496009.21856666665
	,498525.42956666666
	,501674.7545333333
	,504197.08666666667
	,507345.7158333334
	,509865.2856
	,512385.7114666667
	,515538.75786666665
	,518061.9924333333
	,521216.2575333333
	,523741.7463333334
	,526898.6196333334
	,529426.4153666666
	,531957.1346999999
	,535122.4158
	,537654.0189
	,540820.3046333335
	,543353.1055
	,545886.3092666665
	,549053.4182666666
	,551588.0846666667
	,554757.5437333334
	,557292.4032000001
	,559828.7957
	,562997.8541333332
	,565534.2980666666
	,568709.6649999999
	,571249.7172666666
	,573790.0703666667
	,576966.0044666667
	,579505.9694666667
	,582682.2277
	,585223.6823
	,587764.2020666667
	,590940.0571666666
	,593483.1912666665
	,596026.3725
	,599205.4451
	,601746.4072333333
	,604921.6576333332
	,607463.0489333333
	,610007.9545333334
	,613191.4748666667
	,615738.8463666667
	,618922.8917333334
	,621470.0042333334
	,624017.6801333333
	,627203.1910333333
	,629749.1271666667
	,632298.0367666667
	,635482.3311666666
	,638030.0856333333
	,641214.3490333334
	,643760.2273333333
	,646307.8729
	,649497.0210000001
	,652049.6203333334
	,654923.103
};

const UniqCombinedBiasData::InterpolatedData biases =
{
	83406.8496
	,84682.41930000001
	,83634.45623333333
	,81199.51393333334
	,78810.60286666667
	,76467.51383333335
	,74170.48796666665
	,71919.24893333334
	,69714.082
	,67821.95270000001
	,65702.34493333333
	,63885.5339
	,62102.63506666666
	,60108.2482
	,58400.199966666674
	,56725.90813333334
	,55317.122366666656
	,53705.113433333325
	,52126.856366666674
	,50582.32236666667
	,49284.94136666667
	,47802.73733333333
	,46557.904299999995
	,45135.75746666667
	,43941.817233333335
	,42771.77826666667
	,41435.949166666665
	,40315.9233
	,39217.96556666667
	,38142.54403333333
	,37089.19883333333
	,36058.277733333336
	,35049.17343333334
	,34062.1249
	,33096.05733333334
	,32306.924333333332
	,31378.83826666667
	,30472.7792
	,29734.513366666666
	,28865.99296666667
	,28016.77113333333
	,27324.23933333333
	,26510.165033333327
	,25847.112866666674
	,25070.122033333333
	,24435.935166666663
	,23691.133699999995
	,23084.426933333332
	,22490.466500000006
	,21793.981766666664
	,21227.645066666664
	,20673.517299999996
	,20022.387699999996
	,19494.005
	,18976.828166666663
	,18469.24516666666
	,17973.876733333327
	,17394.124166666665
	,16922.693300000003
	,16461.998866666672
	,16011.974100000001
	,15571.476200000005
	,15140.103200000012
	,14719.493699999992
	,14308.852633333338
	,13908.172333333341
	,13515.15756666667
	,13133.78403333333
	,12760.455999999986
	,12396.435366666663
	,12040.231499999994
	,11693.465333333335
	,11354.465466666676
	,11090.853366666668
	,10769.453733333328
	,10454.501933333328
	,10146.728900000007
	,9848.316
	,9556.31733333334
	,9327.816966666656
	,9049.137766666672
	,8777.390533333344
	,8512.237433333328
	,8305.821633333331
	,8053.79806666668
	,7809.146966666663
	,7569.362166666669
	,7383.2939
	,7156.646566666673
	,6935.8204333333315
	,6762.50946666667
	,6551.303400000004
	,6348.243133333327
	,6187.18723333332
	,5992.348100000003
	,5803.44313333333
	,5657.656166666672
	,5480.472466666678
	,5306.102499999989
	,5168.818266666669
	,5002.675199999988
	,4874.487033333319
	,4717.210766666666
	,4564.70683333333
	,4445.208766666678
	,4299.36970000001
	,4184.834699999997
	,4048.0141333333354
	,3942.4174000000057
	,3813.1844666666584
	,3711.159833333329
	,3585.304899999998
	,3466.7620333333325
	,3375.4741666666523
	,3262.3304999999914
	,3173.1775999999954
	,3064.151266666653
	,2980.226999999994
	,2877.6127666666675
	,2798.141666666663
	,2702.8336999999883
	,2625.4702333333166
	,2532.7023666666646
	,2461.626799999998
	,2376.275766666678
	,2307.313100000019
	,2225.89386666668
	,2163.4674666666738
	,2089.325933333341
	,2029.3253666666667
	,1970.1687333333346
	,1899.032300000011
	,1845.8770333333425
	,1783.1310999999987
	,1734.0693999999978
	,1673.3793333333258
	,1627.618499999992
	,1571.457600000004
	,1526.6585000000002
	,1467.1645666666639
	,1422.8285999999982
	,1382.0349666666687
	,1332.7185666666676
	,1292.9295666666683
	,1247.2545333333353
	,1213.5866666666698
	,1167.2158333333402
	,1130.785599999993
	,1095.21146666667
	,1053.2578666666716
	,1020.4924333333329
	,979.7575333333225
	,949.2463333333241
	,911.1196333333113
	,882.9153666666631
	,857.6347000000145
	,827.915800000017
	,803.5189000000051
	,774.8046333333477
	,751.6055000000051
	,728.8092666666489
	,700.9182666666651
	,679.5846666666524
	,654.0437333333539
	,632.9032000000007
	,613.2956999999975
	,587.3541333333123
	,567.7980666666408
	,548.164999999979
	,532.2172666666642
	,516.5703666666523
	,497.5044666666848
	,481.4694666666522
	,462.72769999998854
	,448.1823000000052
	,432.7020666666601
	,413.5571666666656
	,400.69126666665153
	,387.8724999999977
	,371.94510000001173
	,356.90723333331215
	,337.1576333333117
	,322.54893333330983
	,311.4545333333469
	,299.97486666667584
	,291.3463666666842
	,280.39173333333264
	,271.5042333333404
	,263.1801333333521
	,253.69103333332654
	,243.62716666665315
	,236.53676666668616
	,225.83116666666078
	,217.58563333332617
	,206.84903333332235
	,196.72733333332386
	,188.37289999997788
	,182.52100000000792
	,179.12033333334452
	,177.1030000000028
};

}

double UniqCombinedBiasData::getThreshold()
{
	return 177700;
}

const UniqCombinedBiasData::InterpolatedData & UniqCombinedBiasData::getRawEstimates()
{
	return raw_estimates;
}

const UniqCombinedBiasData::InterpolatedData & UniqCombinedBiasData::getBiases()
{
	return biases;
}

}
