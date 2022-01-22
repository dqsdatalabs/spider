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
import dateparser

class MySpider(Spider):
    name = 'domica_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://domica.nl/huurwoningen"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='images']"):
            status = item.xpath(".//span[contains(@class,'product-label')]/text()").get()
            if status and ("verhuurd" in status.lower() or "kijkronde" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath(".//a[@rel='click']/@href").get())
            yield Request(follow_url, callback=self.populate_item)

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rented =  response.xpath("//div[@class='availability']/strong/text()[.='Bezichtigen niet meer mogelijk']").extract_first()
        if rented:
            return

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Domica_PySpider_netherlands")      
        item_loader.add_xpath("title", "//meta[@property='og:title']/@content")

        f_text = "".join(response.xpath("//td[contains(.,'Type object')]/following-sibling::td//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[@id='home']/text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return
        rent = "".join(response.xpath("//div[@class='prices']/div[@class='price']/text()").getall())
        if rent:
            price = rent.split(",")[0].strip().replace(",","").replace(".","")
            item_loader.add_value("rent_string",price.strip())
        else:
            item_loader.add_value("currency","EUR")

        utilities = "".join(response.xpath("//tbody/tr[contains(.,'Servicekosten')]/td[2]/strong/text()").getall())
        if utilities:
            uti = utilities.split(",")[0].strip().replace(",","").replace(".","").split("€")[1].strip()
            if uti.strip() !="0" and uti: 
                item_loader.add_value("utilities",uti)

        item_loader.add_xpath("latitude","//div[@id='maps']/@data-latitude")
        item_loader.add_xpath("longitude","//div[@id='maps']/@data-longitude")


        deposit = "".join(response.xpath("//tbody/tr[contains(.,'Waarborgsom')]/td[2]/strong/text()").getall())
        if deposit:
            dep = deposit.split(",")[0].strip().replace(",","").replace(".","").split("€")[1]          
            if dep.strip() !="0" and dep: 
                item_loader.add_value("deposit",dep.strip())

        room_count = "".join(response.xpath("//div[@class='spec']/i[contains(@class,'fa-bed')]/following-sibling::text()").getall())
        if room_count:
            room = room_count.strip()
            if room !="0":
                item_loader.add_value("room_count",room.strip())
            else:
                studio = "".join(response.xpath("//tr[td[contains(.,'Type object')]]/td[2]/strong/text()").extract())
                if "studio" in studio.lower():
                    item_loader.add_value("room_count","1")
        

        meters = "".join(response.xpath("//div[@class='spec']/i[contains(@class,'fa-arrow')]/following-sibling::text()").getall())
        if meters:
            item_loader.add_value("square_meters",meters.split("m")[0].strip())

        address = "".join(response.xpath("//div[@class='address']/text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        city = "".join(response.xpath("//tr[td[contains(.,'Straat')]]/td[2]/strong/text()").getall())
        if city:
            item_loader.add_value("city",city.strip())

        zipcode = "".join(response.xpath("//tr[td[contains(.,'Postcode')]]/td[2]/strong/text()").getall())
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip())

        description = " ".join(response.xpath("//div[@class='tab-content hidden-xs']/div/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())

        images = [x for x in response.xpath("//div[@class='slide img-fix-object']/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        available_date="".join(response.xpath("//tr[td[contains(.,'Beschikbaar ')]]/td[2]/strong/text()").getall())
        if available_date:
               date2 = available_date.strip()
               if date2:
                date_parsed = dateparser.parse(
                    date2, date_formats=["%m-%d-%Y"]
                )
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)

        pets_allowed = "".join(response.xpath("//div[@class='spec']/i[contains(@class,'fa-cat')]/following-sibling::text()").getall())
        if pets_allowed:
            if "geen" in pets_allowed.lower():
                item_loader.add_value("pets_allowed",False)
            else:
                item_loader.add_value("pets_allowed",True)

        furnished = "".join(response.xpath("//div[@class='spec']/i[contains(@class,'fa-cat')]/following-sibling::text()").getall())
        if furnished:
                item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_phone", "077-3690520")
        item_loader.add_value("landlord_email", "venlo@domica.nl")
        item_loader.add_value("landlord_name", "Domica Rent A Home")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None