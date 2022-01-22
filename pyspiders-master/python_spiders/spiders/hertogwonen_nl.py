# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import scrapy

class MySpider(Spider):
    name = 'hertogwonen_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://hertogwonen.nl/controllers/realestates.php?page={}&searchExpression=&realEstateTypeFilter%5B%5D=Appartement&placeOfResidenceFilter=&priceFilter%5B%5D=0&priceFilter%5B%5D=3000&amountOfBedroomsFilter=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://hertogwonen.nl/controllers/realestates.php?page={}&searchExpression=&realEstateTypeFilter%5B%5D=Vrijstaande+woning&realEstateTypeFilter%5B%5D=Halfvrijstaande+woning&realEstateTypeFilter%5B%5D=Tussenwoning&realEstateTypeFilter%5B%5D=Hoekwoning&placeOfResidenceFilter=&priceFilter%5B%5D=0&priceFilter%5B%5D=3000&amountOfBedroomsFilter=0",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://hertogwonen.nl/controllers/realestates.php?page={}&searchExpression=&realEstateTypeFilter%5B%5D=Studio&placeOfResidenceFilter=&priceFilter%5B%5D=0&priceFilter%5B%5D=3000&amountOfBedroomsFilter=0",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base_url":item})


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)
        seen = False
        
        data = json.loads(response.body)
        for item in data["RealEstates"]:
            if "verhuurd" in item["RentStatus"]:
                continue
            item_loader = ListingLoader(response=response)

            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", "https://hertogwonen.nl/woningaanbod/")
            #sitede detay page olmadigi icin ext link hepsinde ayni olacak tum itemlar alttaki json dan alinacak.

            item_loader.add_value("external_source", "Hertogwonen_PySpider_netherlands")
            if "ObjectCode" in item.keys(): item_loader.add_value("external_id", item["ObjectCode"].strip())
            street = item["StreetName"] if "StreetName" in item.keys() else None
            number = item["HouseNumber"] if "HouseNumber" in item.keys() else None
            city = item["PlaceOfResidence"] if "PlaceOfResidence" in item.keys() else None
            province = item["Province"] if "Province" in item.keys() else None
            zipcode = item["PostalCode"] if "PostalCode" in item.keys() else None
            title_value = item["AddressAddition"] if "AddressAddition" in item.keys() else None
            title = ""
            if street: title += street.strip() + " "
            if title_value: title += "("+ title_value.strip() + ")"
            if title: item_loader.add_value("title", title.strip())
            
            address = ""
            if street: address += street.strip() + " "
            if number: address += number.strip() + " "
            if city:
                address += city.strip() + " "
                item_loader.add_value("city", city.strip())
            if zipcode:
                address += zipcode.strip() + " "
                item_loader.add_value("zipcode", zipcode.strip())
            if province: address += province.strip() + " "
            if address: item_loader.add_value("address", address.strip())

            if "GeneralInformation" in item.keys(): item_loader.add_value("description", " ".join(scrapy.Selector(text=item["GeneralInformation"], type="html").xpath("//text()").getall()).strip())         
            if "TotalSquareMeters" in item.keys(): item_loader.add_value("square_meters", item["TotalSquareMeters"].split('m')[0].strip())
            if "AmountOfBedrooms" in item.keys(): item_loader.add_value("room_count", item["AmountOfBedrooms"])
            if "RentPrice" in item.keys(): 
                item_loader.add_value("rent", item["RentPrice"])
                item_loader.add_value("currency", 'EUR')
            
            from datetime import datetime
            from datetime import date
            import dateparser
            available_date = item["DateOfAvailability"] if "DateOfAvailability" in item.keys() else None
            if available_date:
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
                today = datetime.combine(date.today(), datetime.min.time())
                if date_parsed:
                    result = today > date_parsed
                    if result == True:
                        date_parsed = date_parsed.replace(year = today.year + 1)
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

            if "DepositAmount" in item.keys():
                if item["DepositAmount"].strip():
                    item_loader.add_value("deposit", str(int(float(item["DepositAmount"]))))
            if "ImageUrls" in item.keys(): item_loader.add_value("images", item["ImageUrls"])
            if "ImageUrls" in item.keys(): item_loader.add_value("external_images_count", len(item["ImageUrls"]))
            if "Floor" in item.keys(): item_loader.add_value("floor", "".join(filter(str.isnumeric, item["Floor"])))
            if "ServiceCosts" in item.keys(): item_loader.add_value("utilities", str(int(float(item["ServiceCosts"]))))
            if "PetsAllowed" in item.keys(): item_loader.add_value("pets_allowed", True if 'ja' in item["PetsAllowed"].lower() else False)
            if "AdditionalAttributes" in item.keys():
                if 'parkeren' in item["AdditionalAttributes"].lower(): item_loader.add_value("parking", True)
                if 'balkon' in item["AdditionalAttributes"].lower(): item_loader.add_value("balcony", True)
                if 'lift' in item["AdditionalAttributes"].lower(): item_loader.add_value("elevator", True)

            item_loader.add_value("landlord_name", "Hertog Wonen")
            item_loader.add_value("landlord_phone", "00 31 (0)73-6314856")
            item_loader.add_value("landlord_email", "info@hertogwonen.nl")

            status = item["ForRentOrForSale"]
            if status and "te huur" not in status.lower():
                continue
            yield item_loader.load_item()
            seen = True
        
        if page == 2 or seen:
            base_url = response.meta["base_url"]
            p_url = base_url.format(page)
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "base_url":base_url, "page":page+1})  

       
              
        
        
    

        

              
        