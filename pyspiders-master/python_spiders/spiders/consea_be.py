# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.linkextractors import LinkExtractor
from scrapy import Request 
from scrapy.selector import Selector
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re  
import dateparser  

class MySpider(Spider):
    name = "consea_be" 
    execution_type = 'testing'
    country = 'belgium'
    locale='nl'
    external_source='Consea_PySpider_belgium'
    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.consea.immo/te-huur?searchon=list&sorts=Flat&transactiontype=Rent",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.consea.immo/te-huur?searchon=list&sorts=Dwelling&transactiontype=Rent",
                "property_type" : "house"
            },
        ] # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse, meta={'property_type': url.get('property_type')})
    def parse(self, response):
        for item in  response.xpath("//div[@data-view='showOnList']//a[@class='col-md-4 pand-wrapper clearfix ']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        next_page = response.xpath("//a[@title='Volgende']//@href").get()
        if next_page:        
            yield Request(url=response.urljoin(next_page),callback=self.parse,meta={'property_type': response.meta.get('property_type')})
    
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type",response.meta.get('property_type'))

        title="".join(response.xpath("//div[@class='head-info-main']/h1//text()").getall())
        if title:
            item_loader.add_value("title",re.sub('\s{2,}', ' ', title.strip()))
        address="".join(response.xpath("//td[.='Adres:']/following-sibling::td//text()").getall())
        if address:
            item_loader.add_value("address",address)
        external_id=response.xpath("//td[.='Referentie:']/following-sibling::td/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        rent=response.xpath("//td[.='Huurprijs:']/following-sibling::td/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("/maand")[0].split("€")[-1].replace(".",""))
        item_loader.add_value("currency","EUR")
        energy_label=response.xpath("//td[.='Energielabel:']/following-sibling::td/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)
        room_count=response.xpath("//td[.='Slaapkamers:']/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//td[.='Badkamers:']/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        img=[]
        images=[x for x in response.xpath("//picture//img//@data-srcset").getall()]
        if images:
            for i in images:
                if "http" in i:
                    img.append(i)
                    item_loader.add_value("images",img)
        description="".join(response.xpath("//div[@class='col-xs-12']//p//text()").getall())
        if description:
            item_loader.add_value("description",description)
        elevator=response.xpath("//td[.='Lift:']/following-sibling::td/text()").get()
        if elevator and elevator=="Ja":
            item_loader.add_value("elevator",True)
        parking=response.xpath("//td[.='Garage:']/following-sibling::td/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        item_loader.add_value("landlord_name","Consea Immo")


        yield item_loader.load_item()