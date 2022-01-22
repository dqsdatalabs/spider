# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'immobilier-saphir_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="ImmobilierSaphir_PySpider_france"
    url = "https://www.immobilier-saphir.com/catalog/advanced_search_result.php"

    def start_requests(self):
        start_urls = [
            {
                "formdata" : {
                    'aa_afunc': 'call',
                    'aa_sfunc': 'get_products_search_ajax',
                    'aa_cfunc': 'get_scroll_products_callback',
                    'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":1,"nb_rows_per_page":6,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"1,Loft","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_MIN":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_30_MAX":"","C_34_MIN":"","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_34_MAX":"","C_33_MAX":"","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","keywords":""}',
                },
                "property_type" : "apartment",
            },
            {
                "formdata" : {
                    'aa_afunc': 'call',
                    'aa_sfunc': 'get_products_search_ajax',
                    'aa_cfunc': 'get_scroll_products_callback',
                    'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":1,"nb_rows_per_page":6,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"2,17,30,MaisonVille","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_MIN":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_30_MAX":"","C_34_MIN":"","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_34_MAX":"","C_33_MAX":"","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","keywords":""}',
                },
                "property_type" : "house"
            },
        ]
        for item in start_urls:
            yield FormRequest(self.url,
                            formdata=item["formdata"],
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": item["property_type"], "formdata": item["formdata"]})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//a[@class='link-product not-luxury']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        if page <= 5:
            formdata = response.meta["formdata"]
            formdata["aa_sfunc_args[]"] = formdata["aa_sfunc_args[]"].replace('"page":' + str(page - 1), '"page":' + str(page))
            yield FormRequest(self.url,
                            formdata=formdata,
                            dont_filter=True,
                            callback=self.parse,
                            meta={"property_type": response.meta["property_type"], "formdata": formdata, "page": page + 1})

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title=response.xpath("//h1[@class='product-title']/text()").get()
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//div[.='Loyer mensuel HC']/following-sibling::div/b/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("EUR")[0])
        item_loader.add_value("currency","EUR")

        zipcode=response.xpath("//div[.='Code postal']/following-sibling::div/b/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())
        city=response.xpath("//div[.='Ville']/following-sibling::div/b/text()").get()
        if city:
            item_loader.add_value("city",city.replace("\n","").strip())
        adres=city+" "+zipcode
        if adres:
            item_loader.add_value("address",adres.strip())
        square_meters=response.xpath("//div[.='Surface']/following-sibling::div/b/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].split(".")[0].strip())
        room_count=response.xpath("//span[contains(.,'pièce(s)')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//div[.='Salle(s) de bains']/following-sibling::div/b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        energy_label=response.xpath("//span[.=' Classe énergie']/preceding-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        description=response.xpath("//div[@class='products-description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        utilities=response.xpath("//div[.='Honoraires Locataire']/following-sibling::div/b/text()").get()
        if utilities:
            item_loader.add_value("utilities",utilities.split("EUR")[0].strip())
        deposit=response.xpath("//div[.='Dépôt de Garantie']/following-sibling::div/b/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("EUR")[0].strip())
        available_date=response.xpath("//div[.='Disponibilité']/following-sibling::div/b/text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date)
        terrace=response.xpath("//div[.='Nombre de terrasses']/following-sibling::div/b/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        external_id=response.xpath("//span[contains(.,'Ref.')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1])
        img=[]
        images=[x for x in response.xpath("//div//img/@src").getall()]
        if images:
            for i in images:
                if "office8" in i:
                    img.append(i)
                    item_loader.add_value("images",img)
        balcony=response.xpath("//div[.='Nombre balcons']/following-sibling::div/b/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)
        parking=response.xpath("//div[.='Nombre places parking']/following-sibling::div/b/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        item_loader.add_value("landlord_name","IMMOBILIER SAPHIR BRIENNE")
        item_loader.add_value("landlord_phone","06 08 69 58 52")

        yield item_loader.load_item()