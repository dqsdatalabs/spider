# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'nnw_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.nnw.com.au/?suburb=&type=Unit%2CApartment%2CFlat%2CSemi%2FDuplex%2CBlock+of+Units%2CTerrace&bedrooms=&bathrooms=&min_price=&max_price=&parking=&status=&post_type=nnw_rental&s=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.nnw.com.au/?suburb=&type=House%2CTownhouse%2CVilla&bedrooms=&bathrooms=&min_price=&max_price=&parking=&status=&post_type=nnw_rental&s=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.nnw.com.au/?suburb=&type=Studio&bedrooms=&bathrooms=&min_price=&max_price=&parking=&status=&post_type=nnw_rental&s=",
                ],
                "property_type" : "studio"
            },

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//a[@class='list-thumb_img']"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Nnw_Com_PySpider_australia")  
        item_loader.add_xpath("title", "//header/h3/text()")        
        item_loader.add_xpath("external_id", "//li[div[.='Property ID']]/div[2]/text()")        
        item_loader.add_xpath("room_count", "//li[div[.='Bedrooms']]/div[2]/text()")
        item_loader.add_xpath("bathroom_count", "//li[div[.='Bathrooms']]/div[2]/text()")
        item_loader.add_xpath("deposit", "//li[div[.='Bond']]/div[2]/text()") 
        rent = response.xpath("//li[div[.='Price']]/div[2]/text()").get()
        if rent:
            rent = rent.split("$")[1].lower().split("p")[0].strip()
            item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'USD')
 
        address = response.xpath("//li[div[.='Location']]/div[2]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = ""
            if "Road " in address:
                city = address.split("Road ")[-1].strip()
            elif "Place " in address:
                city = address.split("Place ")[-1].strip()
            elif "Street " in address:
                city = address.split("Street ")[-1].strip()
            else:
                city = address.split(" ")[-1].strip()
            if city and "Park" not in city:
                item_loader.add_value("city", city.strip())

        parking = response.xpath("//li[div[.='Carports' or .='Garages']]/div[2]/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)
        swimming_pool = response.xpath("//li[div[.='Other Features']]/div[2]/text()[contains(.,'Pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        available_date = response.xpath("//li[div[.='Available From']]/div[2]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split("Available")[-1].strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[@class='entry-content']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x.split("url(")[1].split(")")[0] for x in response.xpath("//div[@class='gallery list-gallery']//div[@class='gallery-item']/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'geoLat =') and contains(.,'geoLong =')]/text()").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split("geoLat =")[1].split(";")[0].strip())
            item_loader.add_value("longitude", script_map.split("geoLong =")[1].split(";")[0].strip())
        item_loader.add_xpath("landlord_name", "//div[@class='agent-data']/div[@class='name']/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-data']/div[@class='contact-info phone'][1]/text()")
        item_loader.add_xpath("landlord_email", "//div[@class='agent-data']/div[@class='contact-info email']/text()")
        yield item_loader.load_item()