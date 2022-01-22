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
    name = 'apropos_app'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://apropos.app/search?isSell=false&location=&rent%5Bfrom%5D=&rent%5Bto%5D=&noOfBedrooms=1&propertyAvailableDate=&propertyTypeId=1&furnishingLevelId=&hasHmo=&sortByRent=mostRecent&isAdvancedSearch=false&pageNumber=1&resultsPerPage=10"
                ], 
                "property_type": "studio"
            },
            {
                "url": [
                    "https://apropos.app/search?isSell=false&location=&rent%5Bfrom%5D=&rent%5Bto%5D=&noOfBedrooms=1&propertyAvailableDate=&propertyTypeId=2&furnishingLevelId=&hasHmo=&sortByRent=mostRecent&isAdvancedSearch=false&pageNumber=1&resultsPerPage=10"
                ], 
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://apropos.app/search?isSell=false&location=&rent%5Bfrom%5D=&rent%5Bto%5D=&noOfBedrooms=1&propertyAvailableDate=&propertyTypeId=3&furnishingLevelId=&hasHmo=&sortByRent=mostRecent&isAdvancedSearch=false&pageNumber=1&resultsPerPage=10",
                    "https://apropos.app/search?isSell=false&location=&rent%5Bfrom%5D=&rent%5Bto%5D=&noOfBedrooms=1&propertyAvailableDate=&propertyTypeId=4&furnishingLevelId=&hasHmo=&sortByRent=mostRecent&isAdvancedSearch=false&pageNumber=1&resultsPerPage=10",
                    "https://apropos.app/search?isSell=false&location=&rent%5Bfrom%5D=&rent%5Bto%5D=&noOfBedrooms=1&propertyAvailableDate=&propertyTypeId=5&furnishingLevelId=&hasHmo=&sortByRent=mostRecent&isAdvancedSearch=false&pageNumber=1&resultsPerPage=10"
                ], 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(url=item,
                                callback=self.parse,
                                meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//div[contains(@class,'property-panel list property-item')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type':response.meta.get('property_type')})
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"pageNumber={page-1}", f"pageNumber={page}")
            yield Request(f_url, callback=self.parse, meta={"page": page+1, 'property_type':response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Apropos_App_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("propertyId=")[1].split("&")[0])

        title = response.xpath("//h4[contains(@class,'address')]//text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        address = response.xpath("//h4[contains(@class,'address')]//text()").get()
        if address:
            if address.count(",") == 1:
                city_zipcode = address.split(",")[-1].strip()
                city = city_zipcode.split(" ")[0]
                zipcode = city_zipcode.split(city)[1].strip()
            else:
                city = address.split(",")[-2].strip()
                zipcode = address.split(",")[-1].strip()
                if zipcode.count(" ") == 1:
                    zipcode = zipcode.split(" ")[-1]
                elif zipcode.count(" ") == 2:
                    zipcode_1 = zipcode.split(" ")[-2]
                    zipcode_2 = zipcode.split(" ")[-1]
                    zipcode = zipcode_1 + " "  + zipcode_2
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)

        rent = response.xpath("//div[contains(@class,'prop-detail')]//h4[contains(.,'£')]//text()").get()
        if rent:
            rent = rent.replace("£","").split(".")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//div[contains(@class,'description-panel')]//text()[contains(.,'deposit')]").get()
        if deposit:
            deposit = deposit.split("deposit")[0].replace("£","").split(".")[0].replace(",","")
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'description-panel')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//parent::span//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'photos')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = "".join(response.xpath("//div[contains(@class,'description-panel')]//li[contains(.,'Garage') or contains(.,'Parking') or contains(.,'parking')]//text()").getall())
        if parking:
            item_loader.add_value("parking", True)

        balcony = "".join(response.xpath("//div[contains(@class,'description-panel')]//li[contains(.,'Balcony')]//text()").getall())
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//i[contains(@class,'couch')]//parent::span//text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = "".join(response.xpath("//div[contains(@class,'description-panel')]//li[contains(.,'Lift')]//text()").getall())
        if elevator:
            item_loader.add_value("elevator", True)

        floor = "".join(response.xpath("//div[contains(@class,'description-panel')]//li[contains(.,'floor')]//text()").getall())
        if floor:
            floor = floor.strip().split(" ")[0]
            item_loader.add_value("floor", floor)

        energy_label = "".join(response.xpath("//div[contains(@class,'description-panel')]//li[contains(.,'EPC')]//text()").getall())
        if energy_label:
            energy_label = energy_label.strip().split(" ")[-1]
            item_loader.add_value("energy_label", energy_label)

        item_loader.add_value("landlord_name", "APROPOS")
        item_loader.add_value("landlord_phone", "0333 444 6555")
        item_loader.add_value("landlord_email", "customersupport@apropos.app")
        
        yield item_loader.load_item()