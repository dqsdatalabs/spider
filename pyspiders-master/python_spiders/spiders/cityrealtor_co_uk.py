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
    name = 'cityrealtor_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=BRANCH%5E178868&propertyTypes=flat&primaryDisplayPropertyType=flats&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords=&index=0",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.rightmove.co.uk/property-to-rent/find.html?locationIdentifier=BRANCH%5E178868&propertyTypes=bungalow%2Cdetached%2Cpark-home%2Csemi-detached%2Cterraced&includeLetAgreed=false&mustHave=&dontShow=&furnishTypes=&keywords=&index=0",
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

        page = response.meta.get("page", 24)
        total_result = int(response.xpath("//span[@class='searchHeader-resultCount']/text()").get().strip())
 
        for item in response.xpath("//h2[@class='propertyCard-title']"):
            follow_url = response.urljoin(item.xpath("./../@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        if page <= total_result:
            p_url = response.url.split("&index")[0] + f"&index={page}"
            yield Request(p_url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "page":page+24})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        prop = response.xpath("//div[@class='_2Pr4092dZUG6t1_MyGPRoL']/div/text()").get()
        if "studio" in prop.lower(): item_loader.add_value("property_type", "studio")
        else: item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cityrealtor_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("/")[-1])

        title = " ".join(response.xpath("//title//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            zipcode = title.split(",")[-1]
            item_loader.add_value("title", title)
            item_loader.add_value("zipcode", zipcode)

        address = response.xpath("//h1[contains(@itemprop,'streetAddress')]//text()").get()
        if address:
            if "-" in address:
                address = address.split("-")[0]
            address = address.replace("NO DEPOSIT,","").replace("* NO DEPOSIT*","")
            city = address.split(",")[-1]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())

        # zipcode = response.xpath("").get()
        # if zipcode:
        #     zipcode = zipcode.strip()
        #     item_loader.add_value("zipcode", zipcode)

        # square_meters = response.xpath("").get()
        # if square_meters:
        #     square_meters = square_meters.strip().split("m")[0].split(",")[0]
        #     item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//div[contains(@class,'_1gfnqJ3Vtd1z40MlC0MzXu')]//span//text()").get()
        if rent:
            rent = rent.strip().replace("£","").split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = "".join(response.xpath("//dt[contains(.,'Deposit')]//following-sibling::dd/text()").getall())
        if deposit:
            deposit = deposit.strip().replace("£","").replace(",","").strip()
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//h2[contains(.,'description')]//parent::div//div//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        # prop_type = response.xpath("//div[@class='_2Pr4092dZUG6t1_MyGPRoL']/div/text()[contains(.,'Studio')]").get()
        # if prop_type:
        #     item_loader.add_value("room_count", "1")
        # else:
        room_count = response.xpath("//div[contains(.,'BEDROOMS')]/parent::div/following-sibling::div/div[2]/div/text()").get()
        if room_count:
            room_count = re.findall("\d+",room_count)
            item_loader.add_value("room_count", room_count) 

        bathroom_count = response.xpath("//div[contains(.,'BATHROOMS')]/parent::div/following-sibling::div/div[2]/div/text()").get()
        if bathroom_count:
            bathroom_count = re.findall("\d+",bathroom_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//meta[contains(@property,'image')]//@content").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//dt[contains(.,'available date')]//following-sibling::dd/text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//dt[contains(.,'Furnish type')]//following-sibling::dd/text()[contains(.,'Furnished')]").get()
        if furnished: 
            item_loader.add_value("furnished", True)
        furnishedcheck=item_loader.get_output_value("furnished")
        if not furnishedcheck:
            furnish=response.xpath("//h2[.='Key features']/../ul//li//text()").getall()
            if furnish:
                for i in furnish:
                    if "furnished" in i.lower():
                        item_loader.add_value("furnished", True)


        latitude_longitude = response.xpath("//script[contains(.,'latitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude":')[1].split(',')[0]
            longitude = latitude_longitude.split('longitude":')[1].split(',')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "CITY REALTOR")
        item_loader.add_value("landlord_phone", "020 7790 7702")
        item_loader.add_value("landlord_email", "customersupport@rightmove.co.uk")

        yield item_loader.load_item()
