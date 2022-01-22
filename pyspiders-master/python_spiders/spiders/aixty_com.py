# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json 
import re

class MySpider(Spider):
    name = 'aixty_com'
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    external_source =  "Aixty_PySpider_france" 
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.aixty.com/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.aixty.com/catalog/advanced_search_result.php?action=update_search&search_id=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MAX=&C_33_MAX=&C_38_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&C_36_MAX=&keywords=",
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        prop = response.meta.get('property_type')

        page = response.meta.get('page', 2)
        seen = False
        for item in response.xpath("//div[@class='cell-product']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":prop})

            seen = True
        
        if page == 2 or seen:
            formdata={
                'aa_afunc': 'call',
                'aa_sfunc': 'get_products_search_ajax',
                'aa_cfunc': 'get_scroll_products_callback',
                'aa_sfunc_args[]': '{"type_page":"carto","infinite":true,"sort":"","page":2,"nb_rows_per_page":6,"C_28_search":"EGAL","C_28_type":"UNIQUE","C_28":"Location","C_27_search":"EGAL","C_27_type":"TEXT","C_27":"1","C_65_search":"CONTIENT","C_65_type":"TEXT","C_65":"","C_30_MAX":"","C_34_MIN":"","C_34_search":"COMPRIS","C_34_type":"NUMBER","C_30_MIN":"","C_30_search":"COMPRIS","C_30_type":"NUMBER","C_34_MAX":"","C_33_MAX":"","C_38_MAX":"","C_36_MIN":"","C_36_search":"COMPRIS","C_36_type":"NUMBER","C_36_MAX":"","keywords":""}'
            }
            url = f"https://www.aixty.com/catalog/advanced_search_result.php"
            yield FormRequest(
                url, 
                formdata= formdata,
                callback=self.parse, meta={"page": page+1,"property_type":prop})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//div[contains(@class,'mini-title')]/text()").get()
        item_loader.add_value("property_type",response.meta.get("property_type"))
        if title:
            item_loader.add_value("title",title.replace("\n","").replace("\t",""))
        rent=response.xpath("//span[@class='alur_loyer_price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[0].split("Loyer")[1].replace("\xa0",""))
        item_loader.add_value("currency","GBP")
        external_id=response.xpath("//div[@class='product-model']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.replace("\t","").replace("\n","").split("Référence")[1].strip())
        room_count=response.xpath("//span[contains(.,'pièce(s)')]/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count.strip().split(" ")[0])
        bathroom_count=response.xpath("//span[contains(.,'Salle de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip().split(" ")[0])
        
        square_meters=response.xpath("//span[contains(.,'m²')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0].strip().replace("\xa0",""))
        energy_label=response.xpath("//span[contains(.,'Classe énergie')]/preceding-sibling::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        adres=response.xpath("//h1[@class='product-title']/span/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//div[.='Ville']/following-sibling::div/b/text()").get()
        if city:
            item_loader.add_value("city",city)
        zipcode=response.xpath("//div[.='Code Postal Internet']/following-sibling::div/b/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        images=[x for x in response.xpath("//div[@class='item-slider']/img/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=response.xpath("//div[@class='products-description']/text()").get()
        if description:
            item_loader.add_value("description",description)
        elevator=response.xpath("//div[.='Ascenseur']/following-sibling::div/b/text()").get()
        if elevator and "Oui"==elevator:
            item_loader.add_value("elevator",True)
        terrace=response.xpath("//div[.='Nombre de terrasses']/following-sibling::div/b/text()").get()
        if terrace:
            item_loader.add_value("terrace",True)
        parking=response.xpath("//div[.='Nombre places parking']/following-sibling::div/b/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        item_loader.add_value("landlord_name","Aixty Immobilier")
        yield item_loader.load_item()