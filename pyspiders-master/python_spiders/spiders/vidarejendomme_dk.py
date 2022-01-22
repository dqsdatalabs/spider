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
    name = 'vidarejendomme_dk'
    execution_type = 'testing'
    country = 'denmark'
    locale ='da'
    start_urls = ['https://vidarejendomme.dk/ledige-lejemaal/']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'bolig-item')]//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item) 

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        externalid=response.xpath("//link[@rel='shortlink']/@href").get()
        if externalid:
            item_loader.add_value("external_id",externalid.split("=")[-1])
        
        desc = "".join(response.xpath("//div[contains(@class,'desktopdesc')]//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return
        item_loader.add_value("external_source", "Vidarejendomme_PySpider_denmark")

        title ="".join(response.xpath("//title//text()").get())
        if title:
            title=title.replace("\n","").replace("\t","")
            title=re.sub('\s{2,}',' ',title.strip())
            item_loader.add_value("title", title)
        address =response.xpath("//h1[@class='entry-title']//text()").get()
        if address:
            item_loader.add_value("address", address)
            city=address.split(" ")[0]
            item_loader.add_value("city", city.strip()) 
            zipcode=response.xpath("//h1[@class='entry-title']//text()").get()
            if zipcode:
                zipcode=zipcode.split(",")[-1]
                zip=re.findall("\d+",zipcode)
                item_loader.add_value("zipcode",zip)
        rent =response.xpath("//td[contains(.,'Pris')]/following-sibling::td/text()").get()
        if rent:
            rent=re.findall("\d+",rent)
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency", "DKK") 
        room_count =response.xpath("//td[contains(.,'Værelser')]/following-sibling::td/text()").get()
        if room_count:
            room_count =re.findall("\d+",room_count)
            item_loader.add_value("room_count", room_count)

        description ="".join(response.xpath("//div[@class='col desktopdesc']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        images = [x for x in response.xpath("//img[@class='gimg']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        squaremeters=response.xpath("//td[contains(.,'Størrelse')]/following-sibling::td/text()").get()
        if squaremeters:
            squaremeters=re.findall("\d+",squaremeters)
            item_loader.add_value("square_meters", squaremeters)

        deposit=response.xpath("//td[contains(.,'Depositum')]/following-sibling::td/text()").get()
        if deposit:
            deposit=re.findall("\d+",deposit)
            item_loader.add_value("deposit",deposit)
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//td[contains(.,'Udlejes fra')]/following-sibling::td/text()").get()
        if available_date:
            available_date=available_date.strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        item_loader.add_value("landlord_name", "Vidar Ejendomme")
        item_loader.add_value("landlord_phone", " (+45) 89 37 00 00")
        item_loader.add_value("landlord_email", "mail@vidarejendomme.dk")

        yield item_loader.load_item() 

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("lejlighed" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and (" hus " in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "værelse" in p_type_string.lower():
        return "room"
    else:
        return None