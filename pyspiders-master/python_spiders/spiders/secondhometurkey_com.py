# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader

class MySpider(Spider):
    name = 'secondhometurkey_com'
    execution_type='testing'
    country='turkey'
    locale='tr'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        start_urls = [
            {"url": "https://secondhometurkey.com/tr/properties?estateno=&emlak_kind=47&emlak_type=19&emlak_room=&borough=0&unit=&minprice=&maxprice=&islem=filter&fav=&ind=&week=&ara=Arama&price_ordering=", "property_type": "apartment"},
             {"url": "https://secondhometurkey.com/tr/properties?estateno=&emlak_kind=47&emlak_type=36&emlak_room=&borough=0&unit=&minprice=&maxprice=&islem=filter&fav=&ind=&week=&ara=Arama&price_ordering=", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type'),
                                        'base_url':url.get('url')})
 

    # 1. FOLLOWING
    def parse(self, response):
        base_url = response.meta.get("base_url")
        page = response.meta.get("page", 30)
        seen = False
        for item in response.xpath("//div[@class='property_item']"):
            follow_url = item.xpath(".//div[@class='proerty_content']//a/@href").get()
            let_agreed = item.xpath(".//span[@class='tag_t tag_t_red']/text()").get()
            if let_agreed:
                continue
          
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
         
        if page == 2 or seen:
            url = base_url.replace("properties?",f"properties?plus={page}")
      
            yield Request(url, 
                            callback=self.parse, 
                            meta={"page": page + 30, 
                                    "base_url":base_url,
                                    'property_type': response.meta.get('property_type')})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rent = response.meta.get("rent")
        item_loader.add_value('property_type', response.meta.get('property_type'))
        rent =  response.xpath("//div[@class='hidden-xs hidden-sm']//a//text()[normalize-space()]").get()
        rent_type = response.xpath("//div[@class='feature-p-text']//h3//text()[normalize-space()]").get()

        if rent and rent_type and "Haftal" not in rent_type:
            item_loader.add_value("rent_string", rent.replace(".",""))
        elif rent_type and "Haftal" in rent_type:
            if "TL" in rent:
                rent = rent.split("TL")[0].strip()
                currency = "TRY"
            elif "USD" in rent:
                rent = rent.split("USD")[0].strip()
                currency = "USD"
            else:
                rent = rent.split("EUR")[0].strip()
                currency = "EUR"
            item_loader.add_value("rent", int(rent.replace(".","")))
            item_loader.add_value("currency",currency )

        item_loader.add_value("external_link", response.url)
      
        item_loader.add_value("external_source", "Secondhometurkey_PySpider_turkey")  
        item_loader.add_xpath("title", "//div[@class='col-md-9']//h3/text()")   
        external_id = response.xpath("//div[@class='col-md-3']//h3/text()[contains(.,'Item')]").get()
        if external_id:  
            item_loader.add_value("external_id" , external_id.split("#")[-1])  
    
        address = response.xpath("//div[@class='feature-p-text']/p/text()").get()
        if address:            
            item_loader.add_value("address", address.strip())  
            item_loader.add_value("city", address.split("/")[0].strip())  
      
        room_count  = response.xpath("//tr[td[contains(.,'Oda Sayısı')]]/td[2]/text()[normalize-space()]").get()
        if room_count and "Stüdyo" in room_count:
            item_loader.add_value("room_count","1")
        elif room_count and "+" in room_count:
            room_count=room_count.split("+")
            item_loader.add_value("room_count", str(int(room_count[0])+ int(room_count[1])))
        floor = response.xpath("//tr[td[contains(.,'Bulunduğu kat')]]/td[2]/text()[normalize-space()]").get()
        if floor:
            item_loader.add_value("floor", floor)
        bathroom_count = response.xpath("//div[@class='col-md-9']//span[contains(.,'Banyo')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Banyo")[0])
        square_meters = response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12']//span[contains(.,' m2')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" m2")[0])
    
        description = response.xpath("//div[@class='col-xs-12 col-sm-12 col-md-12 col-lg-12']/p//text()").get()
        if description:
            item_loader.add_value("description", description)
        else:
            description = response.xpath("//div[@class='col-md-3']/div/p/text()").get()
            if description:
                item_loader.add_value("description", description)
        images = [x.split("&path=")[-1] for x in response.xpath("//div[@id='property-d-1']//a//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "Secondhome Turkey")
        item_loader.add_value("landlord_email", "info@secondhometurkey.com")
        item_loader.add_value("landlord_phone", "+90 242 814 43 42")
        furnished = response.xpath("//div[i][contains(.,'Mobilyalı')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        balcony = response.xpath("//tr[td[contains(.,'Balkon')]]/td[2]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)  
        terrace = response.xpath("//tr[td[contains(.,'Teras')]]/td[2]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)  
        parking = response.xpath("//div[i][contains(.,'park alanı')]/text()").get()
        if parking:
            item_loader.add_value("parking", True) 
        swimming_pool = response.xpath("//div[i][contains(.,'Havuz') or contains(.,'havuz') ]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True) 
    

        yield item_loader.load_item()
