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
    name = 'rightmove_co_uk'
    execution_type = 'testing' 
    country = 'united_kingdom'
    locale ='en'
    external_source = "RightMove_PySpider_united_kingdom"
    start_urls = ['https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=BRANCH^178868&index=0']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@data-test='property-img']/@href[contains(.,'properties')]").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = response.xpath("//title/text()").get()
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            print(response.url)
        item_loader.add_value("external_source", self.external_source)

        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("properties/")[-1])


        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        description = response.xpath(
            "//h2[contains(.,'Property description')]//following-sibling::div//text()").getall()
        if description:
            item_loader.add_value("description", description)

        address = response.xpath(
            "//h1[@itemprop='streetAddress']//text()").get()
        if address:
            item_loader.add_value("address", address)
            city = ' '.join(address.split(' ')[-1:]).strip()
            item_loader.add_value("city", city)
        zipcode=item_loader.get_output_value("title")
        if zipcode:
            item_loader.add_value("zipcode",zipcode.split(",")[-1])

        room_count=response.xpath("//div[.='BEDROOMS']/parent::div/following-sibling::div/div[2]/div/text()").get()
        if room_count:
            room_count=re.findall("\d+",room_count)
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//div[.='BATHROOMS']/parent::div/following-sibling::div/div[2]/div/text()").get()
        if bathroom_count:
            bathroom_count=re.findall("\d+",bathroom_count)
            item_loader.add_value("bathroom_count",bathroom_count)
        rent = response.xpath(
            "//span[contains(.,'pcm')]//text()").get()
        if rent:
            price= rent.split("£")[1].split("pcm")[0].replace(",",".")
            item_loader.add_value(
                "rent", price)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath(
            "//dt[contains(.,'Deposit:')]//following-sibling::dd//following-sibling::text()").get()
        if deposit:
            item_loader.add_value(
                "deposit", deposit.replace(",","")) 

        available_date = response.xpath(
            "//dt[contains(.,'Let available date:')]//following-sibling::dd//text()").get()
        if available_date:
            item_loader.add_value(
                "available_date", available_date)

        furnished = response.xpath(
            "//dt[contains(., 'Furnish type: ')]//following-sibling::dd//text()").get()
        if furnished:
            item_loader.add_value(
                "furnished", True)
        else:
            item_loader.add_value(
                "furnished", False)

        images = [response.urljoin(x) for x in response.xpath(
            "//meta[@property='og:image']//@content").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath(
            "//script[contains(., 'latitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(
                '"latitude":')[1].split(',\"')[0]
            longitude = latitude_longitude.split(
                '"longitude":')[1].split(',\"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_phone", "020 7790 7702")
        item_loader.add_value("landlord_name", "Rightmove")


        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None