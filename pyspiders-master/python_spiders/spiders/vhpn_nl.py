# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from enum import unique
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'vhpn_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["http://www.vhpn.nl/?action=search&lng=nl"]
    external_source = "Vhpn_PySpider_netherlands"
    # 1. FOLLOWING
    def parse(self, response):
        # page = response.meta.get("page", 2)
        # seen = False
        for item in response.xpath("//div[@id='search']//a[.='Meer informatie']"): #class=btn
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        
        # if page == 2 or seen:
        #     p_url = f"http://www.vhpn.nl/index.php?action=search&page={page}"
        #     yield Request(
        #         p_url,
        #         callback=self.parse,
        #         meta={"page":page+1})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)

        property_type = response.xpath("//div[@class='description']//text()").get() 
        if get_p_type_string(property_type): 
            item_loader.add_value("property_type",get_p_type_string(property_type))
        else:
            return
        title = response.xpath('//div/h1/text()').get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath('//div/h1/text()').get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-1].strip())
        
        bedrooms = response.xpath("//dl/dt[contains(.,'Slaapkamer')]/following-sibling::dd[1]/text()").get()
        # room_count = int(rooms)
        if bedrooms:            
            item_loader.add_value("room_count", bedrooms)
   
        square_meters = response.xpath("//dl/dt[contains(.,'Woonoppervlakte')]/following-sibling::dd[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(" ")[0])
        
        rent =  response.xpath("//dl/dt[contains(.,'Huurprijs')]/following-sibling::dd[1]/text()").get()
        if rent:
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "EUR")
 
        
        available_date = response.xpath("//dl/dt[contains(.,'Beschikbaar')]/following-sibling::dd[1]/text()[.!='€ 0']").get()
        if available_date:
            if "direct" not in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
         
            
        furnished = response.xpath('//dl/dt[contains(.,"Interieur")]/following-sibling::dd[1]/text()').get()
        if furnished:
            if "Gemeubileerd" in furnished or "Gestoffeerd" in furnished:
                item_loader.add_value("furnished", True)
            
        external_id = response.url.split('=')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id)

        desc = ''.join(response.xpath("//div[@class='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
  
        utilities = response.xpath("//dl/dt[contains(.,'Servicekosten')]/following-sibling::dd[1]/text()[.!='€ 0']").get()
        if utilities:
            item_loader.add_value("utilities", utilities.strip())
        
        
        images = [x for x in response.xpath('//div[@class="slick-slider"]//div[@class="slick-slide-img"]/img/@src').getall() if 'data' not in x]        
        if images:
            item_loader.add_value("images", images)
        
        
        # item_loader.add_value("landlord_name", "VHPN")
        # item_loader.add_value("landlord_phone", "010 7600 761")
        # item_loader.add_value("landlord_email", "info@vhpn.nl")
   
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "etage" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "huis" in p_type_string.lower()):
        return "house"
    else:
        return None