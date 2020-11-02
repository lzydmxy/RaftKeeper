/*Check only p-value first*/
DROP TABLE IF EXISTS welch_ttest;
CREATE TABLE welch_ttest (left Float64, right Float64) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (27.5,27.1), (21.0,22.0), (19.0,20.8), (23.6,23.4), (17.0,23.4), (17.9,23.5), (16.9,25.8), (20.1,22.0), (21.9,24.8), (22.6,20.2), (23.1,21.9), (19.6,22.1), (19.0,22.9), (21.7,20.5), (21.4,24.4);
SELECT '0.021378001462867';
SELECT roundBankers(welchTTest(left, right).2, 16) from welch_ttest;
DROP TABLE IF EXISTS welch_ttest;

CREATE TABLE welch_ttest (left Float64, right Float64) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (30.02,29.89), (29.99,29.93), (30.11,29.72), (29.97,29.98), (30.01,30.02), (29.99,29.98);
SELECT '0.090773324285671';
SELECT roundBankers(welchTTest(left, right).2, 16) from welch_ttest;
DROP TABLE IF EXISTS welch_ttest;

CREATE TABLE welch_ttest (left Float64, right Float64) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (0.010268,0.159258), (0.000167,0.136278), (0.000167,0.122389);
SELECT '0.00339907162713746';
SELECT roundBankers(welchTTest(left, right).2, 16) from welch_ttest;
DROP TABLE IF EXISTS welch_ttest;

/*Check t-stat and p-value and compare it with scipy.stat implementation
  First: a=10, sigma (not sigma^2)=5, size=500
  Second: a=10, sigma = 10, size = 500 */
CREATE TABLE welch_ttest (left Float64, right Float64) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (14.72789,-8.65656), (9.61661,22.98234), (13.57615,23.80821), (3.98392,13.33939), (11.98889,-4.05537), (10.99422,23.5155), (5.44792,-6.45272), (20.29346,17.7903), (7.05926,11.463), (9.22732,5.28021), (12.06847,8.39157), (13.52612,6.02464), (8.24597,14.43732), (9.35245,15.76584), (10.12297,1.54391), (15.80624,1.24897), (13.68613,27.1507), (10.72729,7.71091), (5.62078,15.71846), (6.12229,32.97808), (6.03801,-1.79334), (8.95585,-9.23439), (24.04613,11.27838), (9.04757,0.72703), (2.68263,18.51557), (15.43935,9.16619), (2.89423,17.29624), (4.01423,-1.30208), (4.30568,-3.48018), (11.99948,10.12082), (8.40574,-8.01318), (10.86642,-14.22264), (9.4266,16.58174), (-8.12752,-0.55975), (7.91634,5.61449), (7.3967,1.44626), (2.26431,7.89158), (14.20118,1.13369), (6.68233,-0.82609), (15.46221,12.23365), (7.88467,12.45443), (11.20011,14.46915), (8.92027,13.72627), (10.27926,18.41459), (5.14395,29.66702), (5.62178,1.51619), (12.84383,10.40078), (9.98009,3.33266), (-0.69789,6.12036), (11.41386,11.86553), (7.76863,6.59422), (7.21743,22.0948), (1.81176,1.79623), (9.43762,14.29513), (19.22117,19.69162), (2.97128,-7.98033), (14.32851,5.48433), (7.54959,-2.28474), (3.81545,9.91876), (10.1281,10.64097), (2.48596,0.22523), (10.0461,17.01773), (3.59714,22.37388), (9.73522,14.04215), (18.8077,23.1244), (3.15148,18.96958), (12.26062,8.42663), (5.66707,3.7165), (6.58623,14.29366), (17.30902,23.50886), (9.91391,26.33722), (5.36946,26.72396), (15.73637,13.26287), (16.96281,12.97607), (11.54063,17.41838), (18.37358,8.63875), (11.38255,17.08943), (10.53256,23.15356), (8.08833,-4.4965), (16.27556,7.58895), (2.42969,26.04074), (9.56127,6.84245), (7.32998,20.56287), (9.19511,3.84735), (9.66903,-2.76304), (4.15029,13.1615), (8.83511,8.21954), (14.60617,-3.49943), (14.06143,22.12419), (5.39556,7.08323), (10.11871,16.12937), (10.56619,-0.32672), (14.4462,16.5942), (10.42106,7.68977), (7.75551,11.39484), (11.00418,-5.11987), (4.47226,20.87404), (16.35461,8.01007), (18.55174,3.26497), (11.82044,5.61253), (7.39454,20.69182), (11.27767,0.0296), (6.83827,21.904), (7.76858,22.46572), (15.97614,3.63685), (14.53781,-5.10846), (12.99546,14.86389), (16.91151,5.47188), (9.65012,18.44095), (14.25487,16.71368), (14.03618,6.36704), (2.57382,8.82663), (2.50779,14.6727), (14.24787,7.98383), (13.34666,2.65568), (7.31102,21.45827), (10.22981,11.77948), (17.4435,4.71979), (21.2074,3.17951), (6.64191,13.90226), (18.7086,15.50578), (14.78686,10.8026), (9.85287,16.91369), (4.48263,9.90552), (14.17469,13.87322), (14.4342,4.12366), (19.2481,-3.78985), (3.47165,1.7599), (8.28712,3.43715), (8.81657,-3.45246), (0.92319,23.64571), (20.41106,-4.96877), (6.76127,3.93514), (22.00242,1.49914), (8.66129,12.71519), (10.9929,5.11521), (17.95494,4.79872), (17.20996,20.89391), (12.18888,5.363), (12.14257,8.02765), (15.81243,14.30804), (4.43362,11.49002), (1.17567,14.25281), (15.60881,7.6573), (9.34833,15.49686), (6.33513,3.29327), (-0.83095,2.27236), (12.43268,12.58104), (6.63207,19.19128), (11.96877,15.25901), (14.81029,6.5221), (21.84876,10.10965), (3.75896,12.75249), (6.91307,16.50977), (13.73015,-8.6697), (8.63753,8.28553), (15.71679,1.44315), (1.74565,4.65869), (9.16895,0.98149), (5.70685,0.16623), (5.00117,17.66332), (13.06888,4.35346), (7.51204,6.52742), (15.34885,-1.06631), (5.20264,-5.28454), (8.59043,14.25583), (6.45619,8.74058), (14.61979,1.89553), (11.7075,-0.92959), (14.04901,10.30289), (4.20525,-6.3744), (15.1733,-8.1706), (3.12934,10.95369), (8.08049,4.94384), (15.41273,28.40568), (16.90751,3.7004), (5.86893,2.52363), (7.1086,4.07997), (4.418,7.8849), (12.0614,17.95409), (7.07887,16.67021), (3.61585,11.34377), (11.73001,-0.07446), (10.80449,22.00223), (8.40311,3.31778), (9.91276,18.50719), (16.4164,-3.58655), (5.25034,6.5394), (15.20283,12.40459), (10.42909,16.59866), (9.53888,7.54176), (14.68939,-1.51044), (6.60007,12.69758), (18.31058,2.9842), (7.01885,2.49187), (18.71631,2.04113), (10.50002,-2.46544), (10.7517,15.18368), (4.23224,-0.04058), (2.28924,-0.4127), (8.56059,10.5526), (8.25095,12.03982), (9.15673,12.10923), (13.28409,11.54954), (8.4513,-1.18613), (2.83911,11.30984), (2.79676,23.54105), (9.11055,10.67321), (7.18529,24.09196), (-4.1258,7.5008), (5.28306,12.52233), (6.82757,4.30673), (10.89035,9.35793), (5.24822,4.44472), (11.935,-7.00679), (6.45675,8.56241), (10.18088,23.73891), (4.9932,15.62708), (18.09939,16.09205), (8.11738,12.52074), (5.37883,14.58927), (10.50339,-4.80187), (16.64093,8.47964), (14.77263,7.75477), (13.71385,12.6893), (6.98746,7.14147), (10.74635,12.12654), (5.49432,12.32334), (13.46078,7.98909), (10.67565,3.26652), (9.0291,20.53684), (11.51417,32.3369), (13.07118,19.74911), (9.5049,-4.62897), (8.50611,8.26483), (6.47606,20.88451), (13.06526,-2.12982), (19.08658,25.61459), (9.49741,5.32091), (10.60865,-4.1196), (2.28996,7.57937), (8.12846,21.15847), (5.62241,6.46355), (4.07712,7.74846), (17.98526,19.62636), (9.466,28.34629), (11.38904,26.73919), (5.91826,20.40427), (1.52059,3.03378), (18.79161,10.2537), (18.20669,7.47745), (-1.67829,10.79184), (18.01586,3.91962), (16.31577,19.97973), (7.88281,18.87711), (8.46179,12.56157), (10.31113,11.46033), (14.88377,3.78661), (1.31835,-9.45748), (2.53176,12.06033), (9.48625,-0.74615), (3.97936,13.2815), (11.52319,24.78052), (13.24178,5.83337), (7.58739,17.4111), (10.00959,19.70331), (9.73361,11.78446), (8.35716,-1.366), (1.65491,1.37458), (11.11521,16.31483), (6.08355,32.63464), (10.04582,-3.79736), (11.58237,19.17984), (16.40249,-0.27705), (1.9691,-3.69456), (13.22776,28.38058), (2.67059,-1.36876), (9.83651,-25.63301), (2.12539,3.58644), (9.27114,-6.85667), (9.0699,13.42225), (2.78179,12.04671), (12.49311,28.99468), (12.97662,7.87662), (15.06359,2.61119), (16.91565,-3.56022), (5.92011,1.50022), (5.81304,14.55836), (8.46425,9.35831), (9.48705,16.9366), (4.68191,29.23126), (5.70028,15.31386), (-0.78798,13.46112), (10.03442,7.39667), (15.45433,11.15599), (9.43845,9.80499), (3.05825,22.64923), (6.92126,8.67693), (14.05905,18.67335), (19.71579,-3.19127), (15.0131,22.94716), (4.50386,17.86834), (1.31061,16.98267), (10.81197,15.91653), (14.32942,11.79718), (9.26469,18.50208), (7.27679,8.90755), (22.69295,10.44843), (12.03763,4.67433), (7.34876,6.82287), (16.60689,10.82228), (7.48786,-4.18631), (15.78602,20.3872), (17.21048,11.84735), (13.93482,21.25376), (9.69911,10.55032), (12.24315,12.19023), (10.58131,0.63369), (19.57006,7.92381), (9.8856,17.90933), (11.70302,15.30781), (7.89864,10.01877), (12.24831,0.88744), (16.93707,22.20967), (9.65467,-4.23117), (4.221,21.50819), (15.45229,11.27421), (12.83088,-16.23179), (7.58313,33.43085), (12.895,5.15093), (10.02471,1.34505), (13.36059,6.027), (5.07864,-10.43035), (9.72017,27.45998), (11.05809,19.24886), (15.28528,-4.44761), (13.99834,5.453), (19.26989,12.73758), (9.41846,11.2897), (11.65425,31.032), (8.49638,7.39168), (6.38592,11.95245), (-4.69837,26.279), (12.22061,-1.0255), (9.41331,10.36675), (13.2075,11.58439), (12.97005,27.8405), (11.44352,13.1707), (9.79805,31.39133), (6.93116,27.08301), (10.07691,-2.14368), (22.05892,4.08476), (7.80353,21.5573), (-2.17276,16.69822), (0.61509,7.69955), (8.35842,8.32793), (17.77108,6.49235), (14.70841,-7.3284), (1.27992,10.58264), (15.62699,-6.17006), (9.32914,34.55782), (15.41866,10.93221), (10.82009,44.24299), (3.29902,14.6224), (9.21998,-7.42798), (7.93845,15.52351), (10.33344,11.33982), (12.06399,10.46716), (5.5308,13.0986), (8.38727,-4.25988), (18.11104,9.55316), (8.86565,0.75489), (19.41825,25.99212), (9.52376,-0.81401), (3.94552,3.49551), (9.37587,22.99402), (15.44954,10.99628), (15.90527,23.70223), (13.18927,2.71482), (7.01646,22.82309), (9.06005,31.25686), (9.06431,4.86318), (5.76006,-1.06476), (9.18705,15.10298), (-3.48446,-0.61015), (15.89817,17.81246), (12.94719,-1.55788), (23.69426,18.09709), (17.47755,9.11271), (15.61528,9.94682), (0.54832,-7.33194), (14.32916,-4.67293), (9.55305,21.81717), (13.79891,7.16318), (0.82544,13.25649), (13.34875,13.88776), (9.07614,4.95793), (5.19621,17.65303), (2.1451,14.47382), (9.87726,13.19373), (8.45439,31.86093), (-1.41842,5.73161), (7.93598,10.96492), (11.23151,6.97951), (17.84458,1.75136), (7.02237,10.96144), (10.7842,15.08137), (4.42832,9.95311), (4.45044,7.07729), (1.50938,3.08148), (21.21651,22.37954), (6.2097,8.51951), (6.84354,2.88746), (18.53804,26.73509), (12.01072,-2.88939), (4.8345,-2.82367), (20.41587,-0.35783), (14.48353,14.22076), (8.71116,11.50295), (12.42818,7.10171), (14.89244,8.28488), (8.03033,0.54178), (5.25917,13.8022), (2.30092,15.62157), (10.22504,10.79173), (15.37573,28.18946), (7.13666,30.43524), (4.45018,2.54914), (10.18405,9.89421), (3.91025,13.08631), (14.52304,4.68761), (13.14771,5.61516), (11.99219,22.88072), (9.21345,7.4735), (8.85106,11.27382), (12.91887,2.39559), (15.62308,-3.31889), (11.88034,9.61957), (15.12097,23.01381), (11.58168,-1.23467), (16.83051,9.07691), (5.25405,15.78056), (2.19976,12.28421), (4.56716,9.44888), (16.46053,13.16928), (5.61995,4.33357), (8.67704,2.21737), (5.62789,33.17833), (9.84815,13.25407), (13.05834,-2.47961), (11.74205,6.41401), (3.88393,18.8439), (16.15321,-4.63375), (4.83925,-8.2909), (13.00334,12.18221), (4.4028,-2.95356), (4.35794,19.61659), (4.47478,12.45056), (2.38713,-4.17198), (4.25235,21.9641), (10.87509,11.96416), (9.82411,12.74573), (13.61518,10.47873), (10.25507,12.73295), (4.0335,11.31373), (10.69881,9.9827), (5.70321,5.87138), (6.96244,4.24372), (9.35874,-23.72256), (6.28076,28.41337), (8.29015,4.88103), (6.88653,3.61902), (7.70687,8.93586), (8.2001,16.40759), (6.73415,27.84494), (3.82052,5.6001), (3.94469,14.51379), (15.82384,13.5576), (2.54004,12.92213), (10.74876,3.90686), (12.60517,17.07104), (17.7024,15.84268), (4.6722,17.38777), (13.67341,16.54766), (6.4565,5.94487), (12.95699,17.02804), (4.56912,7.66386), (5.58464,10.43088), (4.0638,6.16059), (13.05559,20.46178), (5.38269,20.02888), (0.16354,20.95949), (7.23962,6.50808), (7.38577,7.22366), (8.50951,8.06659), (13.72574,16.08241), (17.80421,13.83514), (3.01135,-0.33454), (8.02608,12.98848), (14.23847,12.99024);
SELECT '-0.5028215369186904', '0.6152361677168877';
SELECT roundBankers(welchTTest(left, right).1, 16) as t_stat, roundBankers(welchTTest(left, right).2, 16) as p_value from welch_ttest;
DROP TABLE IF EXISTS welch_ttest;

/*Check t-stat and p-value and compare it with scipy.stat implementation
  First: a=10, sigma (not sigma^2)=5, size=500
  Second: a=1, sigma = 12, size = 500 */
CREATE TABLE welch_ttest (left Float64, right Float64) ENGINE = Memory;
INSERT INTO welch_ttest VALUES (4.82025,-2.69857), (6.13896,15.80943), (15.20277,7.31555), (14.15351,3.96517), (7.21338,4.77809), (8.55506,9.6472), (13.80816,-26.41717), (11.28411,-10.85635), (7.4612,-1.4376), (7.43759,-0.96308), (12.9832,2.84315), (-5.74783,5.79467), (12.47114,-3.06091), (15.14223,-14.62902), (3.40603,22.08022), (9.27323,-2.11982), (7.88547,-4.84824), (8.56456,-10.50447), (4.59731,2.4891), (7.91213,9.90324), (7.33894,-22.66866), (21.74811,-0.97103), (11.92111,-16.57608), (0.18828,-3.78749), (10.47314,25.84511), (20.37396,5.30797), (11.04991,-18.19466), (13.30083,11.72708), (14.28065,0.2891), (2.86942,-9.83474), (24.96072,6.69942), (14.20164,18.09604), (18.28769,18.52651), (10.50949,1.38201), (9.22273,7.64615), (11.77608,17.66598), (8.56872,-2.44141), (13.74535,-9.01598), (11.65209,27.69142), (12.51894,4.06946), (17.76256,-15.0077), (13.52122,-10.49648), (8.70796,-4.88322), (6.04749,-25.09805), (16.33064,-4.64024), (8.35636,20.94434), (14.03496,24.12126), (11.05834,-14.10962), (14.49261,10.6512), (2.59383,14.50687), (8.01022,-19.88081), (4.05458,-11.55271), (13.26384,13.16921), (14.62058,16.63864), (10.52489,-24.08114), (8.46357,-9.09949), (6.4147,-10.54702), (9.70071,0.20813), (12.47581,8.19066), (4.38333,-2.70523), (17.54172,-0.23954), (10.12109,7.19398), (7.73186,-7.1618), (14.0279,-7.44322), (11.6621,-17.92031), (17.47045,-1.58146), (15.50223,9.18338), (15.46034,3.25838), (13.39964,-14.30234), (14.98025,1.84695), (15.87912,31.13794), (17.67374,-0.85067), (9.64073,19.02787), (12.84904,-3.09594), (7.70278,13.45584), (13.03156,-5.48104), (9.04512,-22.74928), (15.97014,-8.03697), (8.96389,17.31143), (11.48009,-16.65231), (9.71153,-18.58713), (13.00084,-16.52641), (12.39803,14.95261), (13.08188,12.56762), (5.82244,15.00188), (10.81871,1.85858), (8.2539,2.1926), (7.52114,-2.4095), (9.11488,21.56873), (8.37482,3.35509), (14.48652,-4.98672), (11.42152,35.08603), (16.03111,-10.01602), (13.14057,-3.85153), (-2.26351,-6.81974), (15.50394,19.56525), (14.88603,-9.35488), (13.37257,0.24268), (11.84026,-3.51488), (7.66558,-0.37066), (6.24584,24.20888), (3.6312,-11.73537), (2.7018,0.01282), (5.63656,0.03963), (5.82643,-9.65589), (10.06745,-0.37429), (-0.5831,5.61255), (14.84202,0.49984), (9.5524,-10.15066), (19.71713,-14.54314), (14.23109,16.56889), (8.69105,-7.73873), (5.33742,-3.76422), (7.30372,1.40722), (7.93342,2.28818), (15.20884,-13.12643), (7.53839,5.17082), (13.45311,4.79089), (11.04473,-17.42643), (10.76673,8.72548), (15.44145,-3.70285), (14.06596,16.77893), (9.14873,13.382), (12.88372,19.98418), (8.74994,0.00483), (10.53263,-4.75951), (16.16694,2.35391), (8.37197,21.65809), (3.43739,-9.2714), (4.72799,-18.38253), (9.08802,7.23097), (11.2531,14.97927), (5.16115,-4.02197), (10.20895,-29.8189), (18.70884,-12.8554), (15.88924,-7.60124), (3.38758,-14.90158), (6.46449,-3.31486), (10.21088,31.38144), (14.08458,-8.61288), (15.74508,15.31895), (19.31896,-10.19488), (13.19641,13.796), (11.95409,-0.32912), (10.70718,-0.0684), (1.05245,-30.06834), (10.04772,24.93912), (17.01369,-3.26506), (10.2286,-8.29751), (19.58323,-5.39189), (7.02892,-25.08603), (4.16866,-1.45318), (8.94326,16.72724), (4.99854,-3.38467), (8.88352,-26.00478), (18.65422,7.28369), (17.32328,16.96226), (9.33492,16.5858), (14.94788,10.46583), (8.05863,3.84345), (14.6737,-2.99382), (10.93801,1.42078), (0.54036,-11.0123), (-0.34242,2.09909), (5.89076,1.21064), (3.15189,15.36079), (1.94421,-21.61349), (6.38698,22.7726), (10.50654,10.50512), (8.95362,-6.95825), (6.23711,9.20036), (11.75359,15.66902), (12.42155,3.28098), (-1.55472,-9.05692), (4.6688,0.32882), (10.48087,-1.64934), (11.74615,-4.81406), (9.26822,-5.06006), (7.55517,19.97493), (12.76005,2.88646), (16.47102,-0.34552), (11.31297,7.55186), (14.37437,-22.96115), (2.38799,31.29166), (6.44577,6.18798), (5.07471,-2.52715), (11.55123,-11.58799), (7.76795,14.13596), (10.60116,13.45069), (14.40885,12.15179), (11.58158,3.44491), (8.81648,-8.78006), (12.92299,18.32087), (11.26939,11.91757), (17.95014,-2.00179), (2.95002,10.88411), (17.41959,9.09327), (11.12455,6.62484), (8.78541,8.87178), (14.36413,11.52254), (12.98554,-14.15988), (12.58505,-17.19515), (15.49789,14.03089), (11.70999,-2.4095), (0.65596,-16.83575), (11.08202,2.71469), (14.75752,4.84351), (6.84385,-1.17651), (9.27245,-3.37529), (13.78243,-19.92137), (17.4863,4.48952), (4.01777,-12.4906), (11.82861,-5.65277), (13.86551,8.50819), (6.16591,-19.61261), (8.71589,12.54156), (16.77195,11.06784), (17.23243,-12.59285), (-2.12941,3.43683), (5.66629,-3.00325), (12.45153,12.49082), (1.63971,7.20955), (13.84031,17.6547), (4.6144,15.8619), (5.26169,24.3048), (9.27769,-8.05434), (9.14288,-6.06901), (9.71953,-15.69515), (9.38446,-11.13917), (1.64788,-3.90757), (11.72922,-2.57038), (13.68926,5.14065), (9.42952,17.8497), (12.05574,-8.64665), (9.09148,-18.68331), (5.32273,5.8567), (20.25258,-20.93884), (10.14599,4.40583), (10.82156,14.35985), (5.75736,4.18134), (7.13567,4.3635), (9.29746,9.35428), (5.1618,2.8908), (10.076,16.01017), (21.65669,-1.48499), (13.35486,-9.97949), (6.79957,1.03055), (8.76243,-2.79697), (14.59294,6.85977), (16.90609,4.73213), (10.50337,2.7815), (-0.07923,-2.46866), (13.51648,18.39425), (12.0676,-0.80378), (0.86482,-0.22982), (9.03563,-16.11608), (5.38751,3.0862), (17.16866,3.20779), (2.78702,10.50146), (11.15548,-0.21305), (12.30843,11.21012), (8.04897,-0.99825), (9.95814,18.39633), (11.29308,-3.39003), (14.13032,-0.64411), (21.05877,-1.39932), (3.57386,15.45319), (7.96631,-0.66044), (3.30484,-15.2223), (18.61856,-34.39907), (16.35184,-3.57836), (7.65236,16.82828), (18.02895,1.66624), (9.79458,15.43475), (16.7274,8.17776), (8.84453,5.50486), (13.05709,10.43082), (10.91447,-6.63332), (8.40171,2.28008), (16.95211,16.37203), (11.82194,5.16313), (19.87978,-8.85281), (12.88455,13.26692), (-0.00947,-7.46842), (12.28109,8.43091), (6.96462,-13.18172), (13.75282,-0.72401), (14.39141,22.3881), (11.07193,10.65448), (12.88039,2.81289), (11.38253,10.92405), (21.02707,-8.95358), (7.51955,19.80653), (6.31984,-12.86527), (15.6543,5.38826), (14.80315,-6.83501), (8.38024,-15.7647), (21.7516,-27.67412), (14.31336,8.6499), (15.04703,-4.89542), (5.73787,16.76167), (13.16911,12.84284), (12.40695,-17.27324), (9.88968,-4.18726), (8.46703,-14.62366), (8.70637,-5.49863), (8.03551,-16.22846), (5.9757,10.60329), (12.22951,6.46781), (3.14736,1.70458), (10.51266,10.77448), (18.593,0.8463), (10.82213,13.0482), (7.14216,-4.36264), (6.81154,3.22647), (-0.6486,2.38828), (20.56136,6.7946), (11.35367,-0.25254), (11.38205,1.2497), (17.14,1.6544), (14.91215,4.1019), (15.50207,11.27839), (5.93162,-5.04127), (3.74869,18.11674), (14.11532,0.51231), (7.38954,-0.51029), (5.45764,13.52556), (18.33733,16.10171), (9.91923,5.68197), (2.38991,-2.85904), (14.16756,-8.89167), (2.39791,6.24489), (6.92586,10.85319), (5.32474,-0.39816), (2.28812,3.87079), (5.71718,-3.1867), (5.84197,1.55322), (2.76206,16.86779), (19.05928,-14.60321), (11.51788,-1.81952), (6.56648,-3.11624), (3.35735,1.24193), (7.55948,10.18179), (19.99908,4.69796), (13.00634,0.69032), (18.36886,11.7723), (11.14675,7.62896), (16.72931,9.89741), (12.50106,9.11484), (6.00605,-3.84676), (23.06653,-0.4777), (5.39694,0.95958), (9.53167,-7.95056), (12.76944,-10.97474), (7.20604,-6.54861), (13.25391,34.74933), (13.7341,27.39463), (10.85292,4.18299), (-7.75835,6.02476), (10.29728,-1.99397), (13.70099,1.26478), (10.17959,23.37106), (9.98399,10.49682), (12.69389,-11.04354), (-0.28848,-12.22284), (-2.18319,-9.87635), (13.36378,28.90511), (10.09232,6.77613), (5.49489,0.55352), (5.46156,0.37031), (0.94225,7.1418), (12.79205,3.24897), (10.09593,-1.60918), (6.06218,3.1675), (0.89463,-17.97072), (11.88986,-5.61743), (10.79733,14.1422), (1.51371,14.87695), (2.20967,-4.65961), (15.45732,-0.99174), (16.5262,-2.96623), (5.99724,-9.02263), (8.3613,-17.2088), (15.68183,2.78608), (15.32117,6.74239), (14.15674,4.8524), (6.64553,7.46731), (4.20777,1.04894), (-0.10521,-12.8023), (-0.88169,-17.18188), (1.85913,-5.08801), (9.73673,22.13942), (0.30926,-0.36384), (6.17559,17.80564), (11.76602,7.67504), (5.68385,1.59779), (14.57088,4.10942), (12.81509,0.61074), (9.85682,-14.40767), (12.06376,10.59906), (6.08874,16.57017), (11.63921,-15.17526), (14.86722,-6.98549), (10.41035,-0.64548), (2.93794,3.23756), (12.21841,14.65504), (0.23804,4.583), (3.14845,12.72378), (7.29748,5.26547), (3.06134,0.81781), (13.77684,9.38273), (16.21992,10.37636), (5.33511,10.70325), (9.68959,-0.83043), (9.44169,-7.53149), (18.08012,-9.09147), (4.04224,-19.51381), (8.77918,-28.44508), (10.18324,6.44392), (9.38914,11.10201), (11.76995,-2.86184), (14.19963,8.30673), (6.88817,8.8797), (16.56123,10.68053), (15.39885,15.62919), (5.21241,8.00579), (4.44408,6.4651), (17.87587,-4.50029), (12.53337,18.04514), (13.60916,11.12996), (6.60104,-5.14007), (7.35453,9.43857), (18.61572,3.13476), (6.10437,4.9772), (13.08682,-17.45782), (12.15404,0.05552), (4.90789,-1.90283), (2.13353,2.67908), (12.49593,-2.62243), (11.93056,-3.22767), (13.29408,-8.70222), (5.70038,-23.11605), (8.40271,21.6757), (5.19456,12.70076), (-5.51028,4.4322), (14.0329,11.69344), (10.38365,9.18052), (6.56812,-2.2549), (4.21129,-2.15615), (9.7157,20.29765), (9.88553,-0.29536), (13.45346,15.50109), (4.97752,8.79187), (12.77595,5.11533), (8.56465,-20.44436), (4.27703,-3.00909), (18.12502,-4.48291), (12.45735,21.84462), (12.42912,1.94225), (12.08125,-2.81908), (10.85779,17.19418), (4.36013,-9.33528), (11.85062,-0.17346), (8.47776,0.03958), (9.60822,-35.17786), (11.3069,8.36887), (14.25525,-9.02292), (1.55168,-10.98804), (14.57782,0.29335), (7.84786,4.29634), (9.87774,3.87718), (14.75575,-9.08532), (3.68774,7.13922), (9.37667,-7.62463), (20.28676,-10.5666), (12.10027,4.68165), (8.01819,-3.30172), (18.78158,13.04852), (20.85402,13.45616), (18.98069,2.41043), (16.1429,-0.36501), (9.24047,-15.67383), (14.12487,17.92217), (10.18841,8.42106), (-3.04478,3.22063), (5.7552,-7.31753), (9.30376,21.99596), (11.42837,-36.8273), (6.02364,-20.46391), (8.86984,5.74179), (10.91177,-15.83178), (10.04418,14.90454), (18.10774,-8.84645), (7.49384,3.72036), (9.11556,4.6877), (9.7051,16.35418), (5.23268,3.15441), (9.04647,2.39907), (8.81547,-17.58664), (2.65098,-13.18269);
SELECT '14.971190998235835', '5.898143508382202e-44';
SELECT roundBankers(welchTTest(left, right).1, 16) as t_stat, roundBankers(welchTTest(left, right).2, 16) as p_value from welch_ttest;
DROP TABLE IF EXISTS welch_ttest;
