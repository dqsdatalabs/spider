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
    name = 'pierremarlair_be'
    execution_type='testing'
    country='belgium'
    locale='fr'
    external_source="Pierremarlair_PySpider_belgium"
    def start_requests(self):
        start_urls = [
            {"url": "https://www.pierremarlair.be/oc/liste-proprietes/louer-1.html"},

        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')
                        })
    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")

        for item in response.xpath("//div[@class='container-img']/a"):
            follow_url = item.xpath("./@href").get()
            type=item.xpath("./parent::div/following-sibling::div//p[@class='type']/text()").get()
            if type:
                if "appartement" in type.lower():
                    type="apartment"
                if "maison" in type.lower():
                    type="house"
                if "commercial" in type.lower():
                    return
            yield Request(response.urljoin(follow_url), callback=self.populate_item,meta={"property_type":type})

        pagination = response.xpath("//ul[@class='pagination']//li[3]/a/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2 
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//p[@id='price']/text()").get()
        if status and "rented" in status.lower():
            return
        elif status and "option" in status.lower():
            return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type",response.meta.get("property_type"))
        dontallow=response.xpath("//h1[@class='contentTitleh1']/text()").get()
        if dontallow and "entrepôt" in dontallow.lower():
            return 
        dontallow1=response.xpath("//h1[@class='contentTitleh1']/text()").get()
        if dontallow1 and "commerce" in dontallow1.lower():
            return 
        
        item_loader.add_xpath("title", "//title//text()")
        adres=response.xpath("//p[@id='type']/text()").get()
        if adres:
            item_loader.add_value("address",adres.split("à")[-1].strip())
        cityzipcode=item_loader.get_output_value("address")
        if cityzipcode:
            item_loader.add_value("zipcode",cityzipcode.split(" ")[0])
        if cityzipcode:
            item_loader.add_value("city",cityzipcode.split(" ")[1])
        rent=response.xpath("//p[@id='price']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1])
        item_loader.add_value("currency","EUR")
        square_meters=response.xpath("//li[@id='superficie']/strong/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0])
        room_count=response.xpath("//li[@id='chambre']/strong/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//li[@id='salle-bain']/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        item_loader.add_value("external_source", self.external_source)
        external_id=response.xpath("//p[@id='id-bien']/strong/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        desc=" ".join(response.xpath("//p[@id='description']/text()").getall())
        if desc:
            item_loader.add_value("description",desc)
        images=[x for x in response.xpath("//div[@class='container']//img/@src").getall()]
        if images:
            for i in images:
                if not "templates" in i:
                    item_loader.add_value("images",i)

   

        item_loader.add_value("landlord_phone", "081 840 840")
        item_loader.add_value("landlord_name", "Pierre Marlair")
        item_loader.add_value("landlord_email", "info@pierremarlair.be")    
      

        yield item_loader.load_item()