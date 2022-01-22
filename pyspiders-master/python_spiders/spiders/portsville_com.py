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
from datetime import datetime
class MySpider(Spider):
    name = 'portsville_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    start_urls = ["https://www.portsville.com/properties"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='property']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item)
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        rented = "".join(response.xpath("//div[@class='tag']/text()[contains(.,'Rented')]").extract())
        if rented:
            return
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()") 
        item_loader.add_value("external_source", "Portsville_PySpider_netherlands")

        f_text = "".join(response.xpath("//div[@class='row']/div[contains(.,'Property type')]/following-sibling::*/text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            f_text = "".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
            if get_p_type_string(f_text):
                item_loader.add_value("property_type", get_p_type_string(f_text))
            else:
                return

        rent = "".join(response.xpath("substring-after(//div[@class='price']/h2/text(),': ')").getall())
        if rent:
            price = rent.split(",")[0].strip()
            item_loader.add_value("rent_string",price.strip())
        else:
            item_loader.add_value("currency","EUR") 
        item_loader.add_value("external_id",response.url.split("properties/")[1].split("/")[0].strip()) 

        room_count = "".join(response.xpath("substring-before(//div[@class='amenities']/div/i[@class='fa fa-bed']/following-sibling::p/text(),' ')").getall())
        if room_count:
            item_loader.add_value("room_count",room_count.strip())  

        bathroom_count = "".join(response.xpath("substring-before(//div[@class='amenities']/div/i[@class='fa fa-bath']/following-sibling::p/text(),' ')").getall())
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count.strip())       

        meters = "".join(response.xpath("substring-before(//div[@class='amenities']/div/i[@class='fa fa-home']/following-sibling::p/text(),' ')").getall())
        if meters:
            item_loader.add_value("square_meters",meters.strip())

        a_city = "".join(response.xpath("substring-before(//div[@class='header']//h4/text(),',')").getall()) 
        if a_city:
            city = a_city.strip().split(" ")[-1].strip()
            zipcode = a_city.strip().split(city)[0].strip()
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode)

        LatLng = " ".join(response.xpath("substring-before(substring-after(//script//text()[contains(.,'LatLng')],'LatLng('),', ')").getall()).strip()   
        if LatLng:
            item_loader.add_value("latitude", LatLng)
            item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script//text()[contains(.,'LatLng')],'LatLng('),', '),')')")

        images = [x for x in response.xpath("//div[@class='images']/div/a/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images) 

        address = "".join(response.xpath("concat(//div[@class='header']/div/h1/text(), ' ', //h4/text())").getall()) 
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address.strip()))

        available_date=response.xpath("//div[@class='details']/div/h3/span/text()").get()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d-%m-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        description = " ".join(response.xpath("//div[contains(@class,'description')]/p/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', '').strip())


        furnished = "".join(response.xpath("//div[@class='amenities']/div/i[@class='fa fa-check']/following-sibling::p/text()").getall())
        if furnished:
            item_loader.add_value("furnished",True)
        else:
            f_false = response.xpath("//div[@class='amenities']/div/i[@class='fa fa-times']/following-sibling::p/text()")
            if f_false:
                item_loader.add_value("furnished",False)

        balcony = response.xpath("//div[div[.='Outside space']]/div[2]/text()")
        if balcony:
            if balcony == "Not available":
                item_loader.add_value("balcony",False)
            else:
                item_loader.add_value("balcony",True)

        parking = response.xpath("//div[div[.='Parking']]/div/text()")
        if parking:
            if  "Not available" in parking:
                item_loader.add_value("parking",False)
            else:
                item_loader.add_value("parking",True)

        elevator = response.xpath("//div[@class='row']/div//text()[contains(.,'Elevator')]")
        if elevator:
            item_loader.add_value("elevator",True)

        item_loader.add_value("landlord_phone", "+31(0)10 8208 798")
        item_loader.add_value("landlord_email", "info@portsville.com")
        item_loader.add_value("landlord_name", "PORTSVILLE")

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