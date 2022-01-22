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
    name = 'hakkenbroek_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://hakkenbroek.com/nl/te_huur/?huurprijs_to=100000&koopprijs_to=10000000"]

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'details box')]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            status = item.xpath(".//div/@class[contains(.,'verhuurd')]").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            p_url = f"https://hakkenbroek.com/nl/te_huur/?huurprijs_to=100000&koopprijs_to=10000000&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"page":page+1})
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Hakkenbroek_PySpider_netherlands")
        item_loader.add_xpath("title", "//title/text()")

        f_text = "".join(response.xpath("//li[contains(.,'Type')]/span/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[@id='detail-box']/div/p/text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        desc = "".join(response.xpath("//div[@id='detail-box']/div/p/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        rent = "".join(response.xpath("//div[@id='detail-box']/div/span[@class='price-tag-right']/text()[contains(.,'€')]").getall())
        if rent:
            price = rent.split(",")[0].strip().replace(",","").replace(".","")
            item_loader.add_value("rent_string",price.strip())

        address = "".join(response.xpath("//div[@id='detail-box']/div/h4[1]/text()").getall())
        if address:
            city = response.xpath("//div[@id='detail-box']/div/h4[1]/span/text()").get()
            item_loader.add_value("address", "{} {}".format(city,address.strip()))
            item_loader.add_value("city", city.strip())

        meters = "".join(response.xpath("//ul[@class='specs']/li[contains(.,'Oppervlakte')]/span/text()").getall())
        if meters:
            item_loader.add_value("square_meters",meters.split("m")[0].strip())

        room_count = "".join(response.xpath("//ul[@class='specs']/li[contains(.,'slaapkamers')]/span/text()").getall())
        if room_count:
            room = room_count.strip()
            if room !="0":
                item_loader.add_value("room_count",room.strip())

        bathroom_count = "".join(response.xpath("//ul[@class='specs']/li[contains(.,'badkamers')]/span/text()").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        available_date=response.xpath("//ul[@class='specs']/li[contains(.,'Beschikbaar')]/span/text()").get()
        if available_date:
            date2 =  available_date.strip().replace("Per direct","now")
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        images = [x for x in response.xpath("//div[@id='bigPic']/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images)  

        floor_plan_images = [x for x in response.xpath("//div[@id='bigPic']/a/@href[contains(.,'van_nijen') and contains(.,'van_nijenrodeweg_11_gang_nov20')]").extract()]
        if floor_plan_images is not None:
            if "van_nijenrodeweg_11_gang_nov20" not in floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images) 

        LatLng = "".join(response.xpath("substring-before(substring-after(//script[contains(.,'LatLng')]/text(),'LatLng('),',')").getall())
        if LatLng:
            item_loader.add_value("latitude",LatLng.strip())
            item_loader.add_xpath("longitude","substring-before(substring-after(//script[contains(.,'LatLng')]/text(),','),')')")

        elevator = "".join(response.xpath("//ul[@class='specs']/li[contains(.,'Lift')]/span/text()").getall())
        if elevator:
            if "ja" in elevator.lower():
                item_loader.add_value("elevator",True)

        balcony = "".join(response.xpath("//ul[@class='specs']/li[contains(.,'Balkon')]/span/text()").getall())
        if balcony:
            if "ja" in balcony.lower():
                item_loader.add_value("balcony",True)

        terrace = "".join(response.xpath("//ul[@class='specs']/li[contains(.,'terras')]/span/text()").getall())
        if terrace:
            if "ja" in terrace.lower():
                item_loader.add_value("terrace",True)

        parking = "".join(response.xpath("//ul[@class='specs']/li[contains(.,'Garage')]/span/text()").getall())
        if parking:
            if "0" not in parking:
                item_loader.add_value("parking",True)
            else:
                item_loader.add_value("parking",False)
        else:
            parking = "".join(response.xpath("//ul[@class='specs']/li[contains(.,'Parkeerruimte')]/span/text()").getall())
            if parking:
                if "ja" in parking.lower() :
                    item_loader.add_value("parking",True)


        item_loader.add_value("landlord_phone", "+ 31 (0) 20 489 51 41")
        item_loader.add_value("landlord_name", "HakkenBroek Housing Company")

    
        
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