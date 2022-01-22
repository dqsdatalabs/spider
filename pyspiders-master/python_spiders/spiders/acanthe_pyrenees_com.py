# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
import math



class MySpider(Spider):

    name = 'acanthe_pyrenees_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Acanthe_pyrenees_com_PySpider_france"
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.acanthe-pyrenees.com/location/"
            }

            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse
                                 )


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//article[@class='location_item']/div/div/a/@href").getall():
            f_url = item
            yield Request(
                f_url, 
                callback=self.populate_item, 
                
            )
          
        # nextpage=response.xpath("//li[@class='pagination-next']/a/@href").get()  
        # if nextpage:      
        #     yield Request(
        #         response.urljoin(nextpage),
        #         callback=self.parse,
        #         dont_filter=True,
        #         meta={"property_type":response.meta["property_type"]})
        
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        title=response.xpath("//div/h1/text()").get()
        if title:
            item_loader.add_value("title",title)

        desc = " ".join(response.xpath("//p[contains(text(),'étage')]/text()").getall()).replace("\n"," ")
        if desc:
            item_loader.add_value("description",desc)
            energy_label = desc.strip().split()[-1]
            if len(energy_label) == 1:
                item_loader.add_value("energy_label",energy_label)

            if "balcon" in desc.lower():
                item_loader.add_value("balcony",True)

        rent = response.xpath("//strong[text()='Loyer mensuel']/following-sibling::span/strong/text()").get()
        if rent:
            print("if",rent)
            rent = rent.split()[0]
            item_loader.add_value("rent",rent)
        else:
            print("else",rent)
            rent = response.xpath("//strong[text()='Loyer mensuel']/following-sibling::strong//text()").get()
            rent = rent.split()[0]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")

        utilities = response.xpath("//strong[text()='Provision charges/mois']/following-sibling::span/strong/text()").get()
        if utilities:
            print("if",rent)
            utilities = utilities.split()[0]
            item_loader.add_value("utilities",utilities)

        images = response.xpath("//a[@data-vc-gitem-zone='prettyphotoLink']/@href").getall()
        if images:
            item_loader.add_value("images",images)

        address = " ".join(response.xpath("//p[strong/text()='Loyer mensuel']/preceding-sibling::p/text()").getall()).replace("\n"," ")
        if address:
            item_loader.add_value("address",address)
            zipcode = address.strip().split()[-2]
            if zipcode:
                item_loader.add_value("zipcode",zipcode)

        item_loader.add_value("landlord_phone","05 61 79 34 34")
        item_loader.add_value("landlord_email","luchon@acanthe-pyrenees.com")
        item_loader.add_value("landlord_name","AGENCE ACANTHE PYRÉNÉES")

        if "studio" in str(response.body).lower():
            item_loader.add_value("property_type","studio")
        else:
            item_loader.add_value("property_type","apartment")



        item_loader.add_value("city","LUCHON")




        yield item_loader.load_item()