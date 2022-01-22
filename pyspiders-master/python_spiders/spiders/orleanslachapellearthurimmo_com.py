# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
import re

class MySpider(Spider):
    name = 'orleanslachapellearthurimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Orleanslachapellearthurimmo_PySpider_france_fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.orleans-lachapelle-arthurimmo.com/recherche,basic.htm?transactions=louer&types%5B%5D=appartement&localization=&extends=&min_price=&max_price=&min_surface=&max_surface=", "property_type": "apartment"},
            {"url": "https://www.orleans-lachapelle-arthurimmo.com/recherche,basic.htm?transactions=louer&types%5B%5D=maison&localization=&extends=&min_price=&max_price=&min_surface=&max_surface=", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='before:empty-content before:absolute before:inset-0 before:z-0']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source",self.external_source)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        external_id = "".join(response.xpath("//div[@class='lg:flex items-baseline lg:space-x-8']//div[@class='text-gray-800']//text()[contains(.,'Référence')]").extract())
        if external_id:
            external_id = external_id.split("Référence")[1].split("-")[0]
            external_id=external_id.replace("\n","").replace(" ","")
            item_loader.add_value("external_id", external_id)

        title = "".join(response.xpath("//title//text()").extract())
        if title:
            title.replace("\n","").replace("\r","").replace("\t","").replace("\xa0","")
            item_loader.add_value("title", title)

        rent="".join(response.xpath("//div[@class='text-4xl dark:bg-gradient-black-l bg-gradient-orange-l bg-clip-text text-transparent font-semibold dark:text-[#bfa268]']//text()").get())
        if rent:
            rent = rent.replace(" ","")
            rent = rent.strip().split("€")[0].split(",")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        square_meters=response.xpath("//div[contains(.,'Surface habitable')]//following-sibling::div[@class='text-gray-400']//text()[contains(.,'m')]").get()
        if "," in square_meters:
            square_mt=square_meters.split(',')[0]
            item_loader.add_value("square_meters", square_mt)
        elif square_meters:
            item_loader.add_value("square_meters", square_meters.split('m²')[0].strip())
        
        room_count=response.xpath("//li[@class='flex items-center justify-between']//div[contains(.,'Nombre de pièces')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//li[@class='flex items-center justify-between']//div[contains(.,'Nombre de salles de bain')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        if "(" in title:
            zipcode = title.split("(")[1].split(")")[0]
            item_loader.add_value("zipcode", zipcode)

        desc="".join(response.xpath("//div[@x-ref='content']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc)
            item_loader.add_value("description", desc.replace("\n","").replace("\u00e9","").replace("\r","").replace("\t","").replace("\xa0","").strip())
            
        images = [ x for x in response.xpath("//img[@class='object-cover overflow-hidden object-center h-full w-full']//@src").getall()]
        if images:
            item_loader.add_value("images", images)

       
        utilties="".join(response.xpath("//li[@class='flex justify-between items-center']//div[contains(.,'Charges')]/following-sibling::div/text()").getall())
        if utilties:
            if "€" in utilties:
                item_loader.add_value("utilities", utilties.split('€')[0].strip())

        deposit="".join(response.xpath("//li[@class='flex justify-between items-center']//div[contains(.,'Dépôt de garantie')]/following-sibling::div/text()").getall())
        if deposit:
            if "€" in deposit:
                deposit = deposit.replace(" ","")
                item_loader.add_value("deposit", deposit.split('€')[0].strip())
        
        item_loader.add_value("landlord_name","ARTHURIMMO.COM")
        item_loader.add_value("landlord_phone","02 38 72 20 20")
        item_loader.add_value("landlord_email","orleans-lachapelle@arthurimmo.com")
        yield item_loader.load_item()