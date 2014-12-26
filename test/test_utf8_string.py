# -*- coding: utf-8 -*-
__author__ = 'Ostico <ostico@gmail.com>'
import unittest
import os

os.environ['DEBUG'] = "0"
os.environ['DEBUG_VERBOSE'] = "0"

import pyorient


class CommandTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(CommandTestCase, self).__init__(*args, **kwargs)
        self.client = None
        self.cluster_info = None
        self.class_id1 = None

    def setUp(self):

        self.client = pyorient.OrientDB("localhost", 2424)
        self.client.connect("root", "root")

        db_name = "test_tr"
        try:
            self.client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e)
        finally:
            db = self.client.db_create(db_name, pyorient.DB_TYPE_GRAPH,
                                       pyorient.STORAGE_TYPE_MEMORY)
            pass

        self.cluster_info = self.client.db_open(
            db_name, "admin", "admin", pyorient.DB_TYPE_GRAPH, ""
        )

        self.class_id1 = self.client.command("create class my_v_class extends V")[0]

    def test_utf8_words(self):

        self.client.command( "insert into my_v_class ( 'model') values ('kfjgkfgòàòT:導字社; S:导字社')" )

        res = self.client.command("select from my_v_class")
        assert res[0].model
        assert res[0].model == 'kfjgkfgòàòT:導字社; S:导字社', \
            "model is not equal to 'kfjgkfgòàòT:導字社; S:导字社': '%s'" % res[0].model

        self.client.command( "insert into my_v_class ( 'model') values ('kfj جوزيف المعركة، بعد تم. تلك لإنعدام المعركة،')")

        res = self.client.command("select from my_v_class")
        assert res[0].model
        assert res[0].model == 'kfjgkfgòàòT:導字社; S:导字社', \
            "model is not equal to 'kfjgkfgòàòT:導字社; S:导字社': '%s'" % res[0].model

        assert res[1].model
        assert res[1].model == 'kfj جوزيف المعركة، بعد تم. تلك لإنعدام المعركة،', \
            "model is not equal to 'kfj جوزيف المعركة، بعد تم. تلك لإنعدام المعركة،': '%s'" % res[1].model

    def test_long_utf8_command_string(self):

        #Loren ipsum
        x = self.client.command("insert into my_v_class ( 'model') values ('kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàò---硻禂稢 煘煓瑐 濷瓂癚 腠腶 禖, 禖 溛滁溒 蠸衋醾 煃榃 艎艑 玾珆玸 婸媥媕 瞵瞷矰 踙, 澉 妎岓岕 醳鏻鐆 滱漮 摲 緦膣膗 揈敜敥 甂睮 鶀嚵巆 鷜鷙鷵 僄塓塕 跾 鬞鬠, 痯 釪傛 覿讄讅 峬峿峹 敁柧 瀤瀪璷 巘斖蘱 噅尰崺 楋, 貆賌 蹢鎒鎛 蛚袲褁 踥踕踛 筩嶵嶯幯 捃挸栚 螾褾賹 籔羻 踣, 魡鳱 潧潣瑽 郔镺陯 蒛 釢髟偛 覛谼貆 舝 諨諿, 蜭 獝瘝 錖霒馞 巕氍爟 溗煂獂 鏹鏊 楋 驐鷑鷩 蠿饡驦 紏蚙迻, 樀樛 墐墆墏 嬏嶟樀 跣, 鉾 紞紏 媝寔嵒 褗褆諓 垽娭屔 葝 筩筡 筡絼綒 嬔嬚嬞 鶊鵱鶆, 逯郹酟 郺鋋錋 蒰裧頖 儤嬯 輗 滆 嶵嶯幯 獝瘝磈 燚璒, 羬羭 橁橖澭 柦柋牬 鮂鮐嚃 煃, 漊煻獌 鍆錌雔 杍肜阰 獌 廦廥硻禂稢 藽轚酁 芤咶 歅 鄜酳銪 螾褾賹 浞浧浵 壾 砎粁, 滍 咍垀坽 溮煡煟 紏蚙迻 顲鱭 馺骱 緁 碢禗禈 儋圚墝 礌簨繖, 箷 葍萯 譺鐼霺 僤凘墈 麷劻穋 犝獫 蔍 莦莚虙 儮嬼懫 魆 獝瘝磈 熿熼燛 鶊鵱鶆 躘鑕, 詵貄 壾 偢偣唲 鵁麍儱 桏毢涒 絒翗腏 葠蜄蛖 緷 勓埳, 媔媝 筡絼綒 蟣襋謯 傎圌媔 慛')")
        assert x[0].model
        assert x[0].model == 'kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàò---硻禂稢 煘煓瑐 濷瓂癚 腠腶 禖, 禖 溛滁溒 蠸衋醾 煃榃 艎艑 玾珆玸 婸媥媕 瞵瞷矰 踙, 澉 妎岓岕 醳鏻鐆 滱漮 摲 緦膣膗 揈敜敥 甂睮 鶀嚵巆 鷜鷙鷵 僄塓塕 跾 鬞鬠, 痯 釪傛 覿讄讅 峬峿峹 敁柧 瀤瀪璷 巘斖蘱 噅尰崺 楋, 貆賌 蹢鎒鎛 蛚袲褁 踥踕踛 筩嶵嶯幯 捃挸栚 螾褾賹 籔羻 踣, 魡鳱 潧潣瑽 郔镺陯 蒛 釢髟偛 覛谼貆 舝 諨諿, 蜭 獝瘝 錖霒馞 巕氍爟 溗煂獂 鏹鏊 楋 驐鷑鷩 蠿饡驦 紏蚙迻, 樀樛 墐墆墏 嬏嶟樀 跣, 鉾 紞紏 媝寔嵒 褗褆諓 垽娭屔 葝 筩筡 筡絼綒 嬔嬚嬞 鶊鵱鶆, 逯郹酟 郺鋋錋 蒰裧頖 儤嬯 輗 滆 嶵嶯幯 獝瘝磈 燚璒, 羬羭 橁橖澭 柦柋牬 鮂鮐嚃 煃, 漊煻獌 鍆錌雔 杍肜阰 獌 廦廥硻禂稢 藽轚酁 芤咶 歅 鄜酳銪 螾褾賹 浞浧浵 壾 砎粁, 滍 咍垀坽 溮煡煟 紏蚙迻 顲鱭 馺骱 緁 碢禗禈 儋圚墝 礌簨繖, 箷 葍萯 譺鐼霺 僤凘墈 麷劻穋 犝獫 蔍 莦莚虙 儮嬼懫 魆 獝瘝磈 熿熼燛 鶊鵱鶆 躘鑕, 詵貄 壾 偢偣唲 鵁麍儱 桏毢涒 絒翗腏 葠蜄蛖 緷 勓埳, 媔媝 筡絼綒 蟣襋謯 傎圌媔 慛', \
            "model is not equal to 'kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàò---硻禂稢 煘煓瑐 濷瓂癚 腠腶 禖, 禖 溛滁溒 蠸衋醾 煃榃 艎艑 玾珆玸 婸媥媕 瞵瞷矰 踙, 澉 妎岓岕 醳鏻鐆 滱漮 摲 緦膣膗 揈敜敥 甂睮 鶀嚵巆 鷜鷙鷵 僄塓塕 跾 鬞鬠, 痯 釪傛 覿讄讅 峬峿峹 敁柧 瀤瀪璷 巘斖蘱 噅尰崺 楋, 貆賌 蹢鎒鎛 蛚袲褁 踥踕踛 筩嶵嶯幯 捃挸栚 螾褾賹 籔羻 踣, 魡鳱 潧潣瑽 郔镺陯 蒛 釢髟偛 覛谼貆 舝 諨諿, 蜭 獝瘝 錖霒馞 巕氍爟 溗煂獂 鏹鏊 楋 驐鷑鷩 蠿饡驦 紏蚙迻, 樀樛 墐墆墏 嬏嶟樀 跣, 鉾 紞紏 媝寔嵒 褗褆諓 垽娭屔 葝 筩筡 筡絼綒 嬔嬚嬞 鶊鵱鶆, 逯郹酟 郺鋋錋 蒰裧頖 儤嬯 輗 滆 嶵嶯幯 獝瘝磈 燚璒, 羬羭 橁橖澭 柦柋牬 鮂鮐嚃 煃, 漊煻獌 鍆錌雔 杍肜阰 獌 廦廥硻禂稢 藽轚酁 芤咶 歅 鄜酳銪 螾褾賹 浞浧浵 壾 砎粁, 滍 咍垀坽 溮煡煟 紏蚙迻 顲鱭 馺骱 緁 碢禗禈 儋圚墝 礌簨繖, 箷 葍萯 譺鐼霺 僤凘墈 麷劻穋 犝獫 蔍 莦莚虙 儮嬼懫 魆 獝瘝磈 熿熼燛 鶊鵱鶆 躘鑕, 詵貄 壾 偢偣唲 鵁麍儱 桏毢涒 絒翗腏 葠蜄蛖 緷 勓埳, 媔媝 筡絼綒 蟣襋謯 傎圌媔 慛': '%s'" % x[0].model

        x = self.client.command("select from my_v_class")

        assert x[0].model
        assert x[0].model == 'kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàò---硻禂稢 煘煓瑐 濷瓂癚 腠腶 禖, 禖 溛滁溒 蠸衋醾 煃榃 艎艑 玾珆玸 婸媥媕 瞵瞷矰 踙, 澉 妎岓岕 醳鏻鐆 滱漮 摲 緦膣膗 揈敜敥 甂睮 鶀嚵巆 鷜鷙鷵 僄塓塕 跾 鬞鬠, 痯 釪傛 覿讄讅 峬峿峹 敁柧 瀤瀪璷 巘斖蘱 噅尰崺 楋, 貆賌 蹢鎒鎛 蛚袲褁 踥踕踛 筩嶵嶯幯 捃挸栚 螾褾賹 籔羻 踣, 魡鳱 潧潣瑽 郔镺陯 蒛 釢髟偛 覛谼貆 舝 諨諿, 蜭 獝瘝 錖霒馞 巕氍爟 溗煂獂 鏹鏊 楋 驐鷑鷩 蠿饡驦 紏蚙迻, 樀樛 墐墆墏 嬏嶟樀 跣, 鉾 紞紏 媝寔嵒 褗褆諓 垽娭屔 葝 筩筡 筡絼綒 嬔嬚嬞 鶊鵱鶆, 逯郹酟 郺鋋錋 蒰裧頖 儤嬯 輗 滆 嶵嶯幯 獝瘝磈 燚璒, 羬羭 橁橖澭 柦柋牬 鮂鮐嚃 煃, 漊煻獌 鍆錌雔 杍肜阰 獌 廦廥硻禂稢 藽轚酁 芤咶 歅 鄜酳銪 螾褾賹 浞浧浵 壾 砎粁, 滍 咍垀坽 溮煡煟 紏蚙迻 顲鱭 馺骱 緁 碢禗禈 儋圚墝 礌簨繖, 箷 葍萯 譺鐼霺 僤凘墈 麷劻穋 犝獫 蔍 莦莚虙 儮嬼懫 魆 獝瘝磈 熿熼燛 鶊鵱鶆 躘鑕, 詵貄 壾 偢偣唲 鵁麍儱 桏毢涒 絒翗腏 葠蜄蛖 緷 勓埳, 媔媝 筡絼綒 蟣襋謯 傎圌媔 慛', \
            "model is not equal to 'kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàò---硻禂稢 煘煓瑐 濷瓂癚 腠腶 禖, 禖 溛滁溒 蠸衋醾 煃榃 艎艑 玾珆玸 婸媥媕 瞵瞷矰 踙, 澉 妎岓岕 醳鏻鐆 滱漮 摲 緦膣膗 揈敜敥 甂睮 鶀嚵巆 鷜鷙鷵 僄塓塕 跾 鬞鬠, 痯 釪傛 覿讄讅 峬峿峹 敁柧 瀤瀪璷 巘斖蘱 噅尰崺 楋, 貆賌 蹢鎒鎛 蛚袲褁 踥踕踛 筩嶵嶯幯 捃挸栚 螾褾賹 籔羻 踣, 魡鳱 潧潣瑽 郔镺陯 蒛 釢髟偛 覛谼貆 舝 諨諿, 蜭 獝瘝 錖霒馞 巕氍爟 溗煂獂 鏹鏊 楋 驐鷑鷩 蠿饡驦 紏蚙迻, 樀樛 墐墆墏 嬏嶟樀 跣, 鉾 紞紏 媝寔嵒 褗褆諓 垽娭屔 葝 筩筡 筡絼綒 嬔嬚嬞 鶊鵱鶆, 逯郹酟 郺鋋錋 蒰裧頖 儤嬯 輗 滆 嶵嶯幯 獝瘝磈 燚璒, 羬羭 橁橖澭 柦柋牬 鮂鮐嚃 煃, 漊煻獌 鍆錌雔 杍肜阰 獌 廦廥硻禂稢 藽轚酁 芤咶 歅 鄜酳銪 螾褾賹 浞浧浵 壾 砎粁, 滍 咍垀坽 溮煡煟 紏蚙迻 顲鱭 馺骱 緁 碢禗禈 儋圚墝 礌簨繖, 箷 葍萯 譺鐼霺 僤凘墈 麷劻穋 犝獫 蔍 莦莚虙 儮嬼懫 魆 獝瘝磈 熿熼燛 鶊鵱鶆 躘鑕, 詵貄 壾 偢偣唲 鵁麍儱 桏毢涒 絒翗腏 葠蜄蛖 緷 勓埳, 媔媝 筡絼綒 蟣襋謯 傎圌媔 慛': '%s'" % x[0].model


# x = CommandTestCase('test_long_utf8_comman_string').run()