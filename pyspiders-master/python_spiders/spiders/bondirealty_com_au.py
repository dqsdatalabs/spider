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
    name = 'bondirealty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bondirealty.com.au/rent?listing_cat=rental&category_ids=45&size=96",
                ],
                "property_type" : "apartment",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        for item in response.xpath("//p/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        if response.xpath("//a[.='Next']").get():
            p_url = response.url.split("&page=")[0] + f"&page={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+1,
                }
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Bondirealty_Com_PySpider_australia")   
        deposit_taken = response.xpath("//tr[th[.='Price']]/td/text()[.='Deposit Taken']").get()
        if deposit_taken:
            return   
        item_loader.add_xpath("title","//h2[@class='property-address']/text()")
        item_loader.add_xpath("external_id","//tr[th[.='Property ID']]/td/text()")
        item_loader.add_xpath("room_count", "//div[@class='small']//span[i[@class='icon-realestate-bedrooms']]/span/text()")
        item_loader.add_xpath("bathroom_count", "//div[@class='small']//span[i[@class='icon-realestate-bathrooms']]/span/text()")
        rent = response.xpath("//tr[th[.='Price']]/td/text()").get()
        if rent:
            if "deposit" in rent.lower(): return
            rent = rent.split("$")[-1].lower().split("p")[0].strip().replace(',', '')
            if rent.isdigit():
                item_loader.add_value("rent", int(float(rent)) * 4)
        item_loader.add_value("currency", 'USD')
 
        address = response.xpath("//tr[th[.='Address']]/td/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1].strip()
            item_loader.add_value("city", city.strip()) 
  
        parking = response.xpath("//div[@class='small']//span[i[@class='icon-realestate-garages']]/span/text()").get()
        if parking:
            item_loader.add_value("parking", True) if parking.strip() != "0" else item_loader.add_value("parking", False)

        script_map = response.xpath("//script[contains(.,' L.marker([')]/text()").get()
        if script_map:
            latlng = script_map.split(" L.marker([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        available_date = response.xpath("//tr[th[.='Available']]/td/text()").get()
        if available_date and "now" not in available_date.lower():
            date_parsed = dateparser.parse(available_date.split("Available")[-1].replace("!","").strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        description = " ".join(response.xpath("//div[contains(@class,'property-details-description')]/div//text()[.!='Read more' and .!='Read less']").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        images = [x for x in response.xpath("//div[@id='photoGallery']//div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
      
        item_loader.add_xpath("landlord_name", "//div[@class='agent-card'][1]//h4/a/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='agent-card'][1]//li[i[@class='icon-general-contact-phone']]/a/text()")
        item_loader.add_value("landlord_email", "inspect@bondirealty.com.au")
   
        yield item_loader.load_item()