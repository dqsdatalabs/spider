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

    name = 'leonardauctioneers_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'        
    external_source = "Leonardauctioneers_PySpider_united_kingdom"
  
    def start_requests(self):

        start_urls = [

            # {
            #     "url" : [
            #         "https://www.leonardauctioneers.com/grid?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
            #     ],
            #     "property_type" : "house"
            # },
            {
                "url" : [
                    "https://www.leonardauctioneers.com/property-for-rent",
                ]
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            )

    # 1. FOLLOWING
    def parse(self, response):


        for item in response.xpath("//div[@class='list-item-content']/h2/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        if response.xpath("//li[text()='Let agreed']").get():
            return

        property_type = response.xpath("//tr[th[.='Style']]/td/text()").get()
        if property_type:
            property_type = property_type.lower()
            if "house" in property_type:
                item_loader.add_value("property_type", "house")
            elif "apartment" in property_type or "flat" in property_type:
                item_loader.add_value("property_type", "apartment")
            elif "studio" in property_type or "office" in property_type:
                item_loader.add_value("property_type", "studio")
            elif "room" in property_type:
                item_loader.add_value("property_type", "room")

        # item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1].strip())
        item_loader.add_value("external_source", "Leonardauctioneers_PySpider_united_kingdom") 
        title = " ".join(response.xpath("//h1/span//text()[normalize-space()]").getall()) 
        if title:
            item_loader.add_value("title", title.strip().replace("\n",""))         
        item_loader.add_xpath("room_count", "//tr[th[.='Bedrooms']]/td/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Bathrooms']]/td/text()")
        item_loader.add_xpath("address", "//tr[th[.='Address']]/td/text()")
        item_loader.add_xpath("city", "//span[@class='addr-town']/text()")
        zipcode = " ".join(response.xpath("//span[@class='addr-postcode']/span/text()").getall()) 
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
        
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
        terrace = response.xpath("//tr[th[.='Style']]/td/text()[contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        description = " ".join(response.xpath("//section[@class='listing-additional-info']/p//text()[not(contains(.,'Email') or contains(.,'Phone '))]").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
     
        furnished = response.xpath("//tr[th[.='Furnished']]/td/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        deposit = response.xpath("//section[@class='listing-additional-info']/p//text()[contains(.,'Deposit')]").get()
        if deposit:
            deposit = deposit.split("Deposit")[0].split("£")[-1]
            item_loader.add_value("deposit", deposit)
        else:
            deposit = " ".join(response.xpath("normalize-space(//tr[th[.='Deposit']]/td/text())").extract())
            if deposit:
                deposit = deposit.split("£")[1].replace(",",".").replace("Deposit &","").strip()
                item_loader.add_value("deposit", int(float(deposit)))

        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")
        images = [x for x in response.xpath("//div[@class='slideshow-thumbs']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "LEONARD ESTATE AGENTS")
        item_loader.add_value("landlord_phone", "028 6772 4242")
        item_loader.add_value("landlord_email", "info@leonardgroup.co.uk")

        yield item_loader.load_item()