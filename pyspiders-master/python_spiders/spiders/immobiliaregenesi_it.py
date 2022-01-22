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
    name = 'immobiliaregenesi_it'
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "Immobiliaregenesi_Pyspider_italy"

    # LEVEL 1
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.immobiliaregenesi.it/ricerca/?contract_type_filter=AFFITTO&building_type_filter=Residenziale&house_type_filter=&province_filter=&city_filter=&surface_min_filter=&surface_max_filter=&price_min_filter=&price_max_filter=&obj_code_filter=&obj_ref_filter=",
                ],
                "property_type": "apartment"
            },

        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        for item in  response.xpath("//div[@class='photo']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title","//title//text()")
        external_id=response.xpath("//span[@itemprop='productID']/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(".")[-1].strip())
        rent=response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        parking=response.xpath("//span[.='Box auto']/following-sibling::text()").get()
        if parking and "box" in parking.lower():
            item_loader.add_value("parking",True)

        square_meters=response.xpath("//span[.='Metri Quadri']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        images=[x for x in response.xpath("//img[@itemprop='image']/@src").getall()]
        if images:
            item_loader.add_value("images",images)
        description=" ".join(response.xpath("//div[@id='descit']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        bathroom_count=response.xpath("//span[.='Bagni']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        room_count=response.xpath("//span[.='Locali']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        adres=response.xpath("//span[@itemprop='name']/text()").get()
        if adres:
            item_loader.add_value("city",adres.strip().split(" ")[-1])
        city=response.xpath("//div[@class='sintesiAnnuncio']/text()").get()
        if city:
            item_loader.add_value("address",city)
        terrace=response.xpath("//span[.='Terrazzo']/following-sibling::text()").get()
        if terrace and "si"==terrace: 
            item_loader.add_value("terrace",True)
        elevator=response.xpath("//span[.='Ascensore']/following-sibling::text()").get()
        if elevator:
            item_loader.add_value("elevator",True)
        energy_label=" ".join(response.xpath("//div[@id='descit']//div[@class='ipeString']/text()").getall())
        if energy_label and "classe energetica" in energy_label.lower():
            energy_label=energy_label.split("CLASSE ENERGETICA:")[-1].split("(")[0]
            if energy_label:
                item_loader.add_value("energy_label",energy_label)
        item_loader.add_value("landlord_name","AGENZIA IMMOBILIARE DANIELA GENESI")
        item_loader.add_value("landlord_phone","0637517066")
        yield item_loader.load_item()