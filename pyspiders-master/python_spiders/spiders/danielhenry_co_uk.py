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
    name = 'danielhenry_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.danielhenry.co.uk/search?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.danielhenry.co.uk/search?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
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

        for item in response.xpath("//div[@class='results']/div/div/a[not(@class)]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Danielhenry_Co_PySpider_united_kingdom")          
        item_loader.add_xpath("title","//title/text()")
        item_loader.add_xpath("room_count", "//tr[th[.='Bedrooms']]/td/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Bathrooms']]/td/text()")
        item_loader.add_xpath("address", "//tr[th[.='Address']]/td/text()")
        zipcode = response.xpath("//span[@class='postcode']//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//span[@class='town']/text()").get()
        if city:
            item_loader.add_value("city", city.strip())
        available_date = response.xpath("//tr[th[.='Available From']]/td/text()").get()
        if available_date:
            item_loader.add_value("available_date", dateparser.parse(available_date.strip(), languages=['en']).strftime("%Y-%m-%d"))
      
        rent = "".join(response.xpath("//tr[th[.='Rent']]/td//text()").getall()) 
        if rent:
            if "week" in rent:
                rent = rent.split("£")[-1].lower().split("/")[0].strip().replace(",","")
                item_loader.add_value("rent", int(float(rent)) * 4)
            else:
                rent = rent.split("£")[-1].lower().split("/")[0].strip().replace(",","")
                item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'GBP')
        terrace = response.xpath("//tr[th[.='Style']]/td/text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        energy_label = response.xpath("//tr[th[.='EPC Rating']]/td//a/text()[contains(.,'/')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label[0])
  
        floor = response.xpath("//tr[th[.='Style']]/td/text()[contains(.,'Floor ')]").get()
        if floor:
            item_loader.add_value("floor", floor.split("Floor ")[0].strip())
        description = " ".join(response.xpath("//dt[strong[.='DESCRIPTION:']]/following-sibling::dd[1]//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        
        deposit = response.xpath("//tr[th[.='Deposit']]/td/text()").get()
        if deposit:
            deposit = deposit.split("£")[1].replace(",","")
            item_loader.add_value("deposit", int(float(deposit)) )
        furnished = response.xpath("//tr[th[.='Furnished']]/td/text()").get()
        if not furnished:
            furnished = response.xpath("//li/span[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        parking = response.xpath("//li/span[contains(.,'Parking') or contains(.,'parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")
        images = [x for x in response.xpath("//div[@class='Slideshow-thumbs']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Daniel Henry")
        item_loader.add_value("landlord_email", "waterside@danielhenry.co.uk")
        item_loader.add_xpath("landlord_phone", "//div[@class='text-wrapper']//a[contains(@href,'tel')]/text()")
 
        yield item_loader.load_item()