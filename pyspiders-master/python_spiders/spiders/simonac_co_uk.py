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
    name = 'simonac_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.rightmove.co.uk/api/_search?locationIdentifier=BRANCH%5E46406&numberOfPropertiesPerPage=24&radius=0.0&sortType=6&index=0&includeLetAgreed=false&viewType=LIST&channel=RENT&areaSizeUnit=sqft&currencyCode=GBP&isFetching=false&propertyTypes=flat", "property_type": "apartment"},
	        {"url": "https://www.rightmove.co.uk/api/_search?locationIdentifier=BRANCH%5E46406&numberOfPropertiesPerPage=24&radius=0.0&sortType=6&index=0&propertyTypes=detached%2Csemi-detached%2Cterraced%2Cbungalow&primaryDisplayPropertyType=houses&includeLetAgreed=false&viewType=LIST&channel=RENT&areaSizeUnit=sqft&currencyCode=GBP&isFetching=false", "property_type": "house"},
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 24)
        data = json.loads(response.body)

        if len(data["properties"]) > 0:
            for item in data["properties"]:
                follow_url = response.urljoin(item["propertyUrl"])
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
                
            base_url = response.meta.get("base_url", response.url.replace("index=0","index=page_count"))
            url = base_url.replace("page_count",f"{str(page)}")
            yield Request(url, callback=self.parse, meta={"page": page+24, "base_url":base_url ,"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//h1/text()")

        item_loader.add_value("external_source", "Simonac_Co_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("properties/")[1].split("/")[0])

        rent = response.xpath("//div[@class='_1gfnqJ3Vtd1z40MlC0MzXu']/span/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))

        deposit = "".join(response.xpath("//dt[contains(.,'Deposit')]//following-sibling::dd//text()").getall())
        if deposit:
            deposit = deposit.replace("£","").strip()
            item_loader.add_value("deposit", deposit)

        room = response.xpath("//div[div[.='BEDROOMS']]/div[2]//text()").extract_first()
        if room:
            item_loader.add_value("room_count", room.replace("x",""))

        # bathroom_count = response.xpath("//div[div[.='BATHROOMS']]/div[2]//text()").extract_first()
        # if bathroom_count:
        #     item_loader.add_value("bathroom_count", bathroom_count.replace("x",""))

        available_date=response.xpath("//dt[contains(.,'Let available date')]//following-sibling::dd//text()").get()
        if available_date:

            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        address = ""
        addres = " ".join(response.xpath("//h2[@class='gBaD1fLdPaNTbHErZvX7y']/text()").extract())
        if addres:    
            if "- " in addres:    
                address = addres.split("-")[-1]
            else:
                address = addres

            item_loader.add_value("address", address)   
        
        city_zipcode = response.xpath("//title//text()").get()
        if city_zipcode:
            city = city_zipcode.split(",")[-2].strip()
            zipcode = city_zipcode.split(",")[-1].strip()
            item_loader.add_value("city", city)  
            item_loader.add_value("zipcode", zipcode)  

        images = [
            response.urljoin(x)
            for x in response.xpath("//meta[@property ='og:image']/@content").extract()
        ]
        item_loader.add_value("images", images)

        desc = "".join(response.xpath("//h2[contains(.,'Property description')]//following-sibling::div//div//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        latlong = " ".join(response.xpath("//script[contains(.,'latitude')]/text()").extract())
        if latlong:
            latitude = latlong.split('latitude":')[1].split(",")[0].strip()
            longitude = latlong.split('longitude":')[1].split(",")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)


        furnished = " ".join(response.xpath("//dt[contains(.,'Furnish type')]//following-sibling::dd//text()").extract())
        if furnished:
            if furnished == "Furnished":
                item_loader.add_value("furnished", True) 
            else:
                if furnished == "Unfurnished":
                    item_loader.add_value("furnished", False)

        parking = " ".join(response.xpath("//b[contains(.,'Parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True) 

        item_loader.add_value("landlord_name", "Simon & Co, Rothwell")
        item_loader.add_value("landlord_phone", "01536 418100")


        room_count = response.xpath("//*[@data-testid='svg-bed']/parent::div/following-sibling::div/div/text()").get()
        if room_count:
            room_count = room_count.strip("×")
            item_loader.add_value("room_count",room_count)


        bathroom_count = response.xpath("//*[@data-testid='svg-bathroom']/parent::div/following-sibling::div/div/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip("×")
            item_loader.add_value("bathroom_count",bathroom_count)

        

        yield item_loader.load_item()