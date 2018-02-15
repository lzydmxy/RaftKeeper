!function(e,n){"function"==typeof define&&define.amd?define(n):"object"==typeof exports?module.exports=n():n()(e.lunr)}(this,function(){return function(e){if(void 0===e)throw new Error("Lunr is not present. Please include / require Lunr before this script.");if(void 0===e.stemmerSupport)throw new Error("Lunr stemmer support is not present. Please include / require Lunr stemmer support before this script.");e.hu=function(){this.pipeline.reset(),this.pipeline.add(e.hu.trimmer,e.hu.stopWordFilter,e.hu.stemmer),this.searchPipeline&&(this.searchPipeline.reset(),this.searchPipeline.add(e.hu.stemmer))},e.hu.wordCharacters="A-Za-zªºÀ-ÖØ-öø-ʸˠ-ˤᴀ-ᴥᴬ-ᵜᵢ-ᵥᵫ-ᵷᵹ-ᶾḀ-ỿⁱⁿₐ-ₜKÅℲⅎⅠ-ↈⱠ-ⱿꜢ-ꞇꞋ-ꞭꞰ-ꞷꟷ-ꟿꬰ-ꭚꭜ-ꭤﬀ-ﬆＡ-Ｚａ-ｚ",e.hu.trimmer=e.trimmerSupport.generateTrimmer(e.hu.wordCharacters),e.Pipeline.registerFunction(e.hu.trimmer,"trimmer-hu"),e.hu.stemmer=function(){var n=e.stemmerSupport.Among,r=e.stemmerSupport.SnowballProgram,i=new function(){function e(){return s<=_.cursor}function i(){var e=_.limit-_.cursor;return!!_.find_among_b(w,23)&&(_.cursor=_.limit-e,!0)}function a(){if(_.cursor>_.limit_backward){_.cursor--,_.ket=_.cursor;var e=_.cursor-1;_.limit_backward<=e&&e<=_.limit&&(_.cursor=e,_.bra=e,_.slice_del())}}function t(){_.ket=_.cursor,_.find_among_b(u,44)&&(_.bra=_.cursor,e()&&(_.slice_del(),function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(o,2))&&(_.bra=_.cursor,e()))switch(n){case 1:_.slice_from("a");break;case 2:_.slice_from("e")}}()))}var s,c=[new n("cs",-1,-1),new n("dzs",-1,-1),new n("gy",-1,-1),new n("ly",-1,-1),new n("ny",-1,-1),new n("sz",-1,-1),new n("ty",-1,-1),new n("zs",-1,-1)],o=[new n("á",-1,1),new n("é",-1,2)],w=[new n("bb",-1,-1),new n("cc",-1,-1),new n("dd",-1,-1),new n("ff",-1,-1),new n("gg",-1,-1),new n("jj",-1,-1),new n("kk",-1,-1),new n("ll",-1,-1),new n("mm",-1,-1),new n("nn",-1,-1),new n("pp",-1,-1),new n("rr",-1,-1),new n("ccs",-1,-1),new n("ss",-1,-1),new n("zzs",-1,-1),new n("tt",-1,-1),new n("vv",-1,-1),new n("ggy",-1,-1),new n("lly",-1,-1),new n("nny",-1,-1),new n("tty",-1,-1),new n("ssz",-1,-1),new n("zz",-1,-1)],l=[new n("al",-1,1),new n("el",-1,2)],u=[new n("ba",-1,-1),new n("ra",-1,-1),new n("be",-1,-1),new n("re",-1,-1),new n("ig",-1,-1),new n("nak",-1,-1),new n("nek",-1,-1),new n("val",-1,-1),new n("vel",-1,-1),new n("ul",-1,-1),new n("nál",-1,-1),new n("nél",-1,-1),new n("ból",-1,-1),new n("ról",-1,-1),new n("tól",-1,-1),new n("bõl",-1,-1),new n("rõl",-1,-1),new n("tõl",-1,-1),new n("ül",-1,-1),new n("n",-1,-1),new n("an",19,-1),new n("ban",20,-1),new n("en",19,-1),new n("ben",22,-1),new n("képpen",22,-1),new n("on",19,-1),new n("ön",19,-1),new n("képp",-1,-1),new n("kor",-1,-1),new n("t",-1,-1),new n("at",29,-1),new n("et",29,-1),new n("ként",29,-1),new n("anként",32,-1),new n("enként",32,-1),new n("onként",32,-1),new n("ot",29,-1),new n("ért",29,-1),new n("öt",29,-1),new n("hez",-1,-1),new n("hoz",-1,-1),new n("höz",-1,-1),new n("vá",-1,-1),new n("vé",-1,-1)],m=[new n("án",-1,2),new n("én",-1,1),new n("ánként",-1,3)],k=[new n("stul",-1,2),new n("astul",0,1),new n("ástul",0,3),new n("stül",-1,2),new n("estül",3,1),new n("éstül",3,4)],f=[new n("á",-1,1),new n("é",-1,2)],b=[new n("k",-1,7),new n("ak",0,4),new n("ek",0,6),new n("ok",0,5),new n("ák",0,1),new n("ék",0,2),new n("ök",0,3)],d=[new n("éi",-1,7),new n("áéi",0,6),new n("ééi",0,5),new n("é",-1,9),new n("ké",3,4),new n("aké",4,1),new n("eké",4,1),new n("oké",4,1),new n("áké",4,3),new n("éké",4,2),new n("öké",4,1),new n("éé",3,8)],g=[new n("a",-1,18),new n("ja",0,17),new n("d",-1,16),new n("ad",2,13),new n("ed",2,13),new n("od",2,13),new n("ád",2,14),new n("éd",2,15),new n("öd",2,13),new n("e",-1,18),new n("je",9,17),new n("nk",-1,4),new n("unk",11,1),new n("ánk",11,2),new n("énk",11,3),new n("ünk",11,1),new n("uk",-1,8),new n("juk",16,7),new n("ájuk",17,5),new n("ük",-1,8),new n("jük",19,7),new n("éjük",20,6),new n("m",-1,12),new n("am",22,9),new n("em",22,9),new n("om",22,9),new n("ám",22,10),new n("ém",22,11),new n("o",-1,18),new n("á",-1,19),new n("é",-1,20)],h=[new n("id",-1,10),new n("aid",0,9),new n("jaid",1,6),new n("eid",0,9),new n("jeid",3,6),new n("áid",0,7),new n("éid",0,8),new n("i",-1,15),new n("ai",7,14),new n("jai",8,11),new n("ei",7,14),new n("jei",10,11),new n("ái",7,12),new n("éi",7,13),new n("itek",-1,24),new n("eitek",14,21),new n("jeitek",15,20),new n("éitek",14,23),new n("ik",-1,29),new n("aik",18,26),new n("jaik",19,25),new n("eik",18,26),new n("jeik",21,25),new n("áik",18,27),new n("éik",18,28),new n("ink",-1,20),new n("aink",25,17),new n("jaink",26,16),new n("eink",25,17),new n("jeink",28,16),new n("áink",25,18),new n("éink",25,19),new n("aitok",-1,21),new n("jaitok",32,20),new n("áitok",-1,22),new n("im",-1,5),new n("aim",35,4),new n("jaim",36,1),new n("eim",35,4),new n("jeim",38,1),new n("áim",35,2),new n("éim",35,3)],p=[17,65,16,0,0,0,0,0,0,0,0,0,0,0,0,0,1,17,52,14],_=new r;this.setCurrent=function(e){_.setCurrent(e)},this.getCurrent=function(){return _.getCurrent()},this.stem=function(){var n=_.cursor;return function(){var e,n=_.cursor;if(s=_.limit,_.in_grouping(p,97,252))for(;;){if(e=_.cursor,_.out_grouping(p,97,252))return _.cursor=e,_.find_among(c,8)||(_.cursor=e,e<_.limit&&_.cursor++),void(s=_.cursor);if(_.cursor=e,e>=_.limit)return void(s=e);_.cursor++}if(_.cursor=n,_.out_grouping(p,97,252)){for(;!_.in_grouping(p,97,252);){if(_.cursor>=_.limit)return;_.cursor++}s=_.cursor}}(),_.limit_backward=n,_.cursor=_.limit,function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(l,2))&&(_.bra=_.cursor,e())){if((1==n||2==n)&&!i())return;_.slice_del(),a()}}(),_.cursor=_.limit,t(),_.cursor=_.limit,function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(m,3))&&(_.bra=_.cursor,e()))switch(n){case 1:_.slice_from("e");break;case 2:case 3:_.slice_from("a")}}(),_.cursor=_.limit,function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(k,6))&&(_.bra=_.cursor,e()))switch(n){case 1:case 2:_.slice_del();break;case 3:_.slice_from("a");break;case 4:_.slice_from("e")}}(),_.cursor=_.limit,function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(f,2))&&(_.bra=_.cursor,e())){if((1==n||2==n)&&!i())return;_.slice_del(),a()}}(),_.cursor=_.limit,function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(d,12))&&(_.bra=_.cursor,e()))switch(n){case 1:case 4:case 7:case 9:_.slice_del();break;case 2:case 5:case 8:_.slice_from("e");break;case 3:case 6:_.slice_from("a")}}(),_.cursor=_.limit,function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(g,31))&&(_.bra=_.cursor,e()))switch(n){case 1:case 4:case 7:case 8:case 9:case 12:case 13:case 16:case 17:case 18:_.slice_del();break;case 2:case 5:case 10:case 14:case 19:_.slice_from("a");break;case 3:case 6:case 11:case 15:case 20:_.slice_from("e")}}(),_.cursor=_.limit,function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(h,42))&&(_.bra=_.cursor,e()))switch(n){case 1:case 4:case 5:case 6:case 9:case 10:case 11:case 14:case 15:case 16:case 17:case 20:case 21:case 24:case 25:case 26:case 29:_.slice_del();break;case 2:case 7:case 12:case 18:case 22:case 27:_.slice_from("a");break;case 3:case 8:case 13:case 19:case 23:case 28:_.slice_from("e")}}(),_.cursor=_.limit,function(){var n;if(_.ket=_.cursor,(n=_.find_among_b(b,7))&&(_.bra=_.cursor,e()))switch(n){case 1:_.slice_from("a");break;case 2:_.slice_from("e");break;case 3:case 4:case 5:case 6:case 7:_.slice_del()}}(),!0}};return function(e){return"function"==typeof e.update?e.update(function(e){return i.setCurrent(e),i.stem(),i.getCurrent()}):(i.setCurrent(e),i.stem(),i.getCurrent())}}(),e.Pipeline.registerFunction(e.hu.stemmer,"stemmer-hu"),e.hu.stopWordFilter=e.generateStopWordFilter("a abban ahhoz ahogy ahol aki akik akkor alatt amely amelyek amelyekben amelyeket amelyet amelynek ami amikor amit amolyan amíg annak arra arról az azok azon azonban azt aztán azután azzal azért be belül benne bár cikk cikkek cikkeket csak de e ebben eddig egy egyes egyetlen egyik egyre egyéb egész ehhez ekkor el ellen elsõ elég elõ elõször elõtt emilyen ennek erre ez ezek ezen ezt ezzel ezért fel felé hanem hiszen hogy hogyan igen ill ill. illetve ilyen ilyenkor ismét ison itt jobban jó jól kell kellett keressünk keresztül ki kívül között közül legalább legyen lehet lehetett lenne lenni lesz lett maga magát majd majd meg mellett mely melyek mert mi mikor milyen minden mindenki mindent mindig mint mintha mit mivel miért most már más másik még míg nagy nagyobb nagyon ne nekem neki nem nincs néha néhány nélkül olyan ott pedig persze rá s saját sem semmi sok sokat sokkal szemben szerint szinte számára talán tehát teljes tovább továbbá több ugyanis utolsó után utána vagy vagyis vagyok valaki valami valamint való van vannak vele vissza viszont volna volt voltak voltam voltunk által általában át én éppen és így õ õk õket össze úgy új újabb újra".split(" ")),e.Pipeline.registerFunction(e.hu.stopWordFilter,"stopWordFilter-hu")}});