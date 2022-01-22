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

class MySpider(Spider):
    name = 'watson1889_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'  
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.watson1889.com/grid?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.watson1889.com/grid?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=8&stygrp=9&stygrp=6",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='list-item-content']/h2/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1].strip())
        item_loader.add_value("external_source", "Watson1889_PySpider_united_kingdom")          
        item_loader.add_xpath("title","//title/text()")
        item_loader.add_xpath("room_count", "//tr[th[.='Bedrooms']]/td/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Bathrooms']]/td/text()")
        item_loader.add_xpath("address", "//tr[th[.='Address']]/td//text()[normalize-space()]")
        zipcode = " ".join(response.xpath("//h1//span[@class='addr-postcode']/span/text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//h1//span[@class='addr-town']//text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        available_date = response.xpath("//tr[th[.='Available From']]/td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        rent = "".join(response.xpath("//tr[th[.='Rent']]/td//text()").getall()) 
        if rent:
            item_loader.add_value("rent_string", rent)
        terrace = response.xpath("//tr[th[.='Style']]/td/text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        parking = response.xpath("//li[contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        energy_label = response.xpath("//tr[th[.='EPC Rating']]/td//a/text()[contains(.,'/')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label[0])
        description = " ".join(response.xpath("//section[@class='listing-short-description']//p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        floor = response.xpath("//tr[th[.='Style']]/td/text()[contains(.,'Floor ')]").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor ")[0].strip())
  
        deposit = response.xpath("//tr[th[.='Deposit']]/td/text()").get()
        if deposit:
            deposit = deposit.split("£")[1].replace(",","")
            item_loader.add_value("deposit", int(float(deposit)) )
        furnished = response.xpath("//tr[th[.='Furnished']]/td/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")
        images = [x for x in response.xpath("//div[@class='slideshow-thumbs']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "WATSON PROPERTY")
        item_loader.add_value("landlord_phone", "028 9050 2845")
        item_loader.add_value("landlord_email", "office@watson1889.com")

        yield item_loader.load_item()