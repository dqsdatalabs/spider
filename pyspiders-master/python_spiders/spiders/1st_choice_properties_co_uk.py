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
    name = '1st_choice_properties_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-apartment/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-flat/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-maisonette/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-new-apartment/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-land/status-available",
                    "https://www.1st-choice-properties.co.uk/properties/lettings/tag-flat/status-available"
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-bungalows/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-detached/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-detached-house/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-house/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-new-home/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-semi-detached/from-200/up-to-1000/status-available",
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-village-house/status-available",

                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.1st-choice-properties.co.uk/search?channel=lettings&fragment=tag-studio/status-available",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='propList-inner']/div/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[@class='pagination_next']/@href").get()
        if next_page:
            p_url = response.urljoin(next_page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"]})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("external_source", "1st_Choice_Properties_Co_PySpider_united_kingdom")
        
        item_loader.add_xpath("title", "//h2[@id='secondary-address']/text()")
        address = response.xpath("//h2[@id='secondary-address']/text()").get()
        if address:
            item_loader.add_value("address", address)
        external_id = response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Ref:')]//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
       
        rent = "".join(response.xpath("//h3[@id='propertyPrice']/text()").getall())
        if rent:
            if "pw" in rent.lower():
                rent = rent.lower().split('£')[-1].split('pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
            else:
                rent = rent.lower().split('£')[-1].split('p')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", rent)     
        
        item_loader.add_value("currency", "GBP")
    
        available_date ="".join(response.xpath("//p[strong[contains(.,'available on')]]/text()").getall())
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        room_count = response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Bedroom')]//text()").get()
        if room_count:        
            room_count = room_count.replace("Bedroom","")
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count = response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Bathroom')]//text()").get()
        if bathroom_count:        
            bathroom_count = bathroom_count.split("Bathroom")[0].strip().split(" ")[-1]
            item_loader.add_value("bathroom_count",bathroom_count)

        script_map = response.xpath("//script[contains(.,'setPosition(new google.maps.LatLng(')]//text()").get()
        if script_map:
            latlng = script_map.split("setPosition(new google.maps.LatLng(")[1].split(")")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
       
        desc = " ".join(response.xpath("//div[@id='propertyDetails']/p[not(contains(.,'Property available on:'))]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
                    
        images = [ response.urljoin(x) for x in response.xpath("//div[@id='carousel_contents']/a/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "1st Choice Properties")
        item_loader.add_value("landlord_phone","020 7681 8181")
        yield item_loader.load_item()