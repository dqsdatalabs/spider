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
    name = 'studentlettingco_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['https://studentlettingco.co.uk/search-results/?lang=']  # LEVEL 1

    formdata = {
        "location_level1": "",
        "beds": "",
        "propertytype2": "",
    }
    
    def start_requests(self):
        start_urls = [
            {
                "type": [
                    "Flat / Apartment / Studio",
                ],
                "property_type": "apartment"
            },
	        {
                "type": [
                    "House",
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('type'):
                self.formdata["propertytype2"] = item
                yield FormRequest(
                    url=self.start_urls[0],
                    dont_filter=True,
                    formdata=self.formdata,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='listingblocksection']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@rel='next']/@href").get()
        if next_page:
            yield FormRequest(
                next_page,
                dont_filter=True,
                formdata=self.formdata,
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Studentletting_Co_PySpider_united_kingdom")

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h2[contains(@id,'title')]//text()").get()
        city = ""
        if address:
            if address.count(",") == 2:
                city = address.split(",")[-2].strip()
            else:
                city = response.xpath("//h3[contains(@class,'detailpagesubheading')]//text()").get()
                if city:
                    if "birmingham" in city.lower():
                        city= "Birmingham"
                    elif "edgbaston" in city.lower():
                        city = "Edgbaston"
            item_loader.add_value("address", address.strip())
        else:
            address = response.xpath("//h3[contains(@class,'detailpagesubheading')]//text()").get()
            if address:
                item_loader.add_value("address", address)

        if city:
            item_loader.add_value("city", city.strip())
        
        external_id = response.xpath("//link[@rel='shortlink']/@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split("p=")[1])
        
        zipcode = response.xpath("//h3[contains(@class,'detailpagesubheading')]//text()").get()
        if zipcode:
            zipcode = zipcode.strip()
            if "," in zipcode:
                zipcode = zipcode.split(",")[-1].strip()
                item_loader.add_value("zipcode", zipcode)
            else:
                zipcode = response.xpath("//h2[contains(@id,'title')]//text()").get()
                if zipcode:
                    zipcode = zipcode.strip()
                    if ", " in zipcode:
                        zipcode = zipcode.split(",")[-1].strip()
                        item_loader.add_value("zipcode", zipcode)

        rent_week = response.xpath("//th[contains(@class,'price')]//text()[contains(.,'Week')]").get()
        if rent_week:
            rent = response.xpath("//table[contains(@class,'property-table')]//td[contains(.,'£')]//text()").get()
            if rent:
                rent = rent.split("£")[1].split(".")[0].strip()
                item_loader.add_value("rent", int(rent)*4)
        else:
            rent = response.xpath("//table[contains(@class,'property-table')]//td[contains(.,'£')]//text()").get()
            if rent:
                rent = rent.split("£")[1].split(".")[0].strip()
                item_loader.add_value("rent", rent)
            else:
                rent = response.xpath("//p[contains(.,'Prices for 2021 – 2022:')]//following-sibling::ul//li//strong//text()").get()
                if rent:
                    rent = rent.split("£")[1].strip()
                    item_loader.add_value("rent", int(rent)*4)
                elif response.xpath("//strong[contains(.,'Prices for 2021/2022')]//following-sibling::p//strong//text()").get():
                    rent = response.xpath("//strong[contains(.,'Prices for 2021/2022')]//following-sibling::p//strong//text()").get()
                    if rent:
                        rent = rent.split("£")[1].strip().split(" ")[0]
                        item_loader.add_value("rent", int(rent)*4)
                else:
                    rent = "".join(response.xpath("//p[contains(.,'Rent')]//text()").getall())
                    if rent and "£" in rent:
                        if "per week" in rent:
                            rent = rent.split("£")[1].split(".")[0].strip()
                            rent = int(rent)*4
                        else:
                            rent = rent.split("£")[1].split(".")[0].strip()
                        item_loader.add_value("rent", rent)

        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@id,'listingcontent')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = " ".join(response.xpath("//div[contains(@id,'listingcontent')]//div//text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

        item_loader.add_value("property_type", "room")
        item_loader.add_value("room_count", "1")

        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split(":")[1].strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'carousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Student Letting Company")
        item_loader.add_value("landlord_phone", "0121 456 5156")
        item_loader.add_value("landlord_email", "info@studentlettingco.co.uk")

        availability = response.xpath("//p//text()[contains(.,'Available')]").get()
        if availability:
            availability = availability.strip().split(" ")[-1]
            if availability < "2020":
                return

        status = response.xpath("//h2[contains(.,'FULLY LET')]/text()").get()
        if not status:
            yield item_loader.load_item()