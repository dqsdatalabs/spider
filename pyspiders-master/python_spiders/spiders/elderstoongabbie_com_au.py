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
from datetime import datetime
from python_spiders.helper import ItemClear
import re
import dateparser

class MySpider(Spider):
    name = 'elderstoongabbie_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.elderstoongabbie.com.au/properties-for-lease/{}?property_type%5B%5D=Studio&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "studio",
            },
            {
                "url" : [
                    "https://www.elderstoongabbie.com.au/properties-for-lease/{}?property_type%5B%5D=House&property_type%5B%5D=Semi%20Detached&property_type%5B%5D=Unit&property_type%5B%5D=Villa&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'listing-item position-relative')]/div/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True

        if page == 2 or seen:
            base = response.meta["base"]
            slug = f"page/{page}/"
            p_url = base.format(slug)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Elderstoongabbie_Com_PySpider_australia")      
        item_loader.add_xpath("title","//h1/text()")
   
        item_loader.add_xpath("room_count", "//li[label[.='Bedrooms']]/div/text()")
        item_loader.add_xpath("bathroom_count", "//li[label[.='Bathrooms']]/div/text()")
        item_loader.add_xpath("external_id", "//li[label[.='Property ID']]/div/text()")

        deposit = response.xpath("//li[label[.='Bond Amount']]/div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",",""))
        city = response.xpath("//div[@class='property-address-wrapper']//h3[@class='sub-title']/text()").get()
        if city:
            item_loader.add_value("city", city)
        address = response.xpath("//li[label[.='Location']]/div/text()").get()
        if address:
            item_loader.add_value("address", address)
        parking = response.xpath("//li[label[.='Parking' or .='Garage' ]]/div/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)

        rent = response.xpath("//div[@class='property-address-wrapper']//div[@class='property-price']/text()").get()
        if rent:
            rent = rent.split("$")[-1].strip().split("p")[0].replace(",","").split(" ")[0]
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        desc = " ".join(response.xpath("//div[@class='detail-description']//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date = response.xpath("//li[label[.='Available From']]/div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [x for x in response.xpath("//div[@class='single-slideshow']//div//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        script_map = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if script_map:
            latlng = script_map.split("L.marker([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        item_loader.add_value("landlord_name", "Elders Real Estate Toongabbie")
        item_loader.add_value("landlord_phone", "02 9896 2333")
        yield item_loader.load_item()
