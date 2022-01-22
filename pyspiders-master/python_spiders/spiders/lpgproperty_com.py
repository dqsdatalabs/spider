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
    name = 'lpgproperty_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
  
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.lpgproperty.com/property-to-rent",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url["url"]:
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url['property_type']})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'PropBox-content')]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        # next_button = response.xpath("//a[contains(@class,'-next')]/@href").get()
        # if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Lpgproperty_PySpider_united_kingdom")          
        item_loader.add_xpath("title","//h1//span[@class='Address-addressLine1']/text()")
        item_loader.add_xpath("room_count", "//tr[th[.='Bedrooms']]/td/text()")
        item_loader.add_xpath("bathroom_count", "//tr[th[.='Bathrooms']]/td/text()")
        address = " ".join(response.xpath("//tr[th[.='Address']]/td//text()[normalize-space()]").getall())
        if address:
            item_loader.add_value("address", address.split(" - ")[-1].strip().replace("\n",","))
        zipcode = " ".join(response.xpath("//h1//span[@class='Address-addressPostcode']/span/text()").getall())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        city = response.xpath("//h1//span[@class='Address-addressTown']//text()").get()
        if city:
            item_loader.add_value("city", city.replace(",","").strip())
        available_date = response.xpath("//tr[th[.='Available From']]/td/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
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
        parking = response.xpath("//section[@class='ListingDescr']//p//text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        energy_label = response.xpath("//tr[th[.='EPC Rating']]/td//a/text()[contains(.,'/')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label[0])
  
        description = " ".join(response.xpath("//section[@class='ListingDescr']//p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
        
        deposit = response.xpath("//tr[th[.='Deposit']]/td/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit )
        furnished = response.xpath("//tr[th[.='Furnished']]/td/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")
        images = [x for x in response.xpath("//div[@class='Slideshow-thumbs']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "LPG Property")
        item_loader.add_value("landlord_phone", "028 7083 3641")
        item_loader.add_value("landlord_email", "paul@lpgproperty.com")
        yield item_loader.load_item()

