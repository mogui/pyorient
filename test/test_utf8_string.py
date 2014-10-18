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
        self.client.connect("admin", "admin")

        db_name = "test_tr"
        try:
            self.client.db_drop(db_name)
        except pyorient.PyOrientCommandException as e:
            print(e.message)
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

    def test_long_utf8_command_string(self):

        x = self.client.command("insert into my_v_class ( 'model') values ('kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàòT:導字社; S:导字社àfgafpglaàgfplàaùèòùèlùèfpgladùègfkadùfblodùfbl,ùgèbp,gèùbsg,ùbèsp,ùègfpb,sùèfbgps,ùfgàpgièeoimaàdkàma4rm4tm5【公司介绍】 北京文海阳苝化学是国内较早立足于化学发光染料研究的专业公司，从90年代起就合成开发了全色系列的化学发光用荧光染料。从2000年起为了突破国外专利的封锁集中力量开始了苝类化学染料的合成研究，并取得了丰硕的成果。随着我们对基础原料四氯苝酐的大规模合成，我们在提高质量和产量的同时也发展了许多国内外客户，尤其为国内科研单位取代苝衍生物的研发提供了优质价廉的原料。在取代苝衍生物的开发工作上我们也进行了工业化量产，特别是8C、18C取代苝酰胺的合成和应用上我们取得了重大的突破，在化学发光中替代了昂贵的Lumaogen-F RDE 300染料。在与大专院校的科研开发合作中我们还成功的开发出了苝类红外化学发光染料，其高度稳定的性能使红外化学发光取得了突破的进展。随着电子行业、特种染色技术、有机太阳能电池技术的发展，苝化学技术越来越得到业界的重视和青睐，文海阳苝化学也将更加努力的为广大科研单位和使用单位服务，同时不断发展自身技术水平，丰富产品系列')")
        assert x[0].model
        assert x[0].model == 'kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàòT:導字社; S:导字社àfgafpglaàgfplàaùèòùèlùèfpgladùègfkadùfblodùfbl,ùgèbp,gèùbsg,ùbèsp,ùègfpb,sùèfbgps,ùfgàpgièeoimaàdkàma4rm4tm5【公司介绍】 北京文海阳苝化学是国内较早立足于化学发光染料研究的专业公司，从90年代起就合成开发了全色系列的化学发光用荧光染料。从2000年起为了突破国外专利的封锁集中力量开始了苝类化学染料的合成研究，并取得了丰硕的成果。随着我们对基础原料四氯苝酐的大规模合成，我们在提高质量和产量的同时也发展了许多国内外客户，尤其为国内科研单位取代苝衍生物的研发提供了优质价廉的原料。在取代苝衍生物的开发工作上我们也进行了工业化量产，特别是8C、18C取代苝酰胺的合成和应用上我们取得了重大的突破，在化学发光中替代了昂贵的Lumaogen-F RDE 300染料。在与大专院校的科研开发合作中我们还成功的开发出了苝类红外化学发光染料，其高度稳定的性能使红外化学发光取得了突破的进展。随着电子行业、特种染色技术、有机太阳能电池技术的发展，苝化学技术越来越得到业界的重视和青睐，文海阳苝化学也将更加努力的为广大科研单位和使用单位服务，同时不断发展自身技术水平，丰富产品系列', \
            "model is not equal to 'kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàòT:導字社; S:导字社àfgafpglaàgfplàaùèòùèlùèfpgladùègfkadùfblodùfbl,ùgèbp,gèùbsg,ùbèsp,ùègfpb,sùèfbgps,ùfgàpgièeoimaàdkàma4rm4tm5【公司介绍】 北京文海阳苝化学是国内较早立足于化学发光染料研究的专业公司，从90年代起就合成开发了全色系列的化学发光用荧光染料。从2000年起为了突破国外专利的封锁集中力量开始了苝类化学染料的合成研究，并取得了丰硕的成果。随着我们对基础原料四氯苝酐的大规模合成，我们在提高质量和产量的同时也发展了许多国内外客户，尤其为国内科研单位取代苝衍生物的研发提供了优质价廉的原料。在取代苝衍生物的开发工作上我们也进行了工业化量产，特别是8C、18C取代苝酰胺的合成和应用上我们取得了重大的突破，在化学发光中替代了昂贵的Lumaogen-F RDE 300染料。在与大专院校的科研开发合作中我们还成功的开发出了苝类红外化学发光染料，其高度稳定的性能使红外化学发光取得了突破的进展。随着电子行业、特种染色技术、有机太阳能电池技术的发展，苝化学技术越来越得到业界的重视和青睐，文海阳苝化学也将更加努力的为广大科研单位和使用单位服务，同时不断发展自身技术水平，丰富产品系列': '%s'" % x[0].model

        x = self.client.command("select from my_v_class")

        assert x[0].model
        assert x[0].model == 'kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàòT:導字社; S:导字社àfgafpglaàgfplàaùèòùèlùèfpgladùègfkadùfblodùfbl,ùgèbp,gèùbsg,ùbèsp,ùègfpb,sùèfbgps,ùfgàpgièeoimaàdkàma4rm4tm5【公司介绍】 北京文海阳苝化学是国内较早立足于化学发光染料研究的专业公司，从90年代起就合成开发了全色系列的化学发光用荧光染料。从2000年起为了突破国外专利的封锁集中力量开始了苝类化学染料的合成研究，并取得了丰硕的成果。随着我们对基础原料四氯苝酐的大规模合成，我们在提高质量和产量的同时也发展了许多国内外客户，尤其为国内科研单位取代苝衍生物的研发提供了优质价廉的原料。在取代苝衍生物的开发工作上我们也进行了工业化量产，特别是8C、18C取代苝酰胺的合成和应用上我们取得了重大的突破，在化学发光中替代了昂贵的Lumaogen-F RDE 300染料。在与大专院校的科研开发合作中我们还成功的开发出了苝类红外化学发光染料，其高度稳定的性能使红外化学发光取得了突破的进展。随着电子行业、特种染色技术、有机太阳能电池技术的发展，苝化学技术越来越得到业界的重视和青睐，文海阳苝化学也将更加努力的为广大科研单位和使用单位服务，同时不断发展自身技术水平，丰富产品系列', \
            "model is not equal to 'kfjgk§°#ù]*[//5%$£@çlàèl0ào0àà=°àkàp0===Kkkòòàojàinònlkbnòkhvlyiyli77glfgòàòT:導字社; S:导字社àfgafpglaàgfplàaùèòùèlùèfpgladùègfkadùfblodùfbl,ùgèbp,gèùbsg,ùbèsp,ùègfpb,sùèfbgps,ùfgàpgièeoimaàdkàma4rm4tm5【公司介绍】 北京文海阳苝化学是国内较早立足于化学发光染料研究的专业公司，从90年代起就合成开发了全色系列的化学发光用荧光染料。从2000年起为了突破国外专利的封锁集中力量开始了苝类化学染料的合成研究，并取得了丰硕的成果。随着我们对基础原料四氯苝酐的大规模合成，我们在提高质量和产量的同时也发展了许多国内外客户，尤其为国内科研单位取代苝衍生物的研发提供了优质价廉的原料。在取代苝衍生物的开发工作上我们也进行了工业化量产，特别是8C、18C取代苝酰胺的合成和应用上我们取得了重大的突破，在化学发光中替代了昂贵的Lumaogen-F RDE 300染料。在与大专院校的科研开发合作中我们还成功的开发出了苝类红外化学发光染料，其高度稳定的性能使红外化学发光取得了突破的进展。随着电子行业、特种染色技术、有机太阳能电池技术的发展，苝化学技术越来越得到业界的重视和青睐，文海阳苝化学也将更加努力的为广大科研单位和使用单位服务，同时不断发展自身技术水平，丰富产品系列': '%s'" % x[0].model


# x = CommandTestCase('test_long_utf8_comman_string').run()