# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json


class MySpider(Spider):
    name = 'parkerwilliams_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'en'
    def start_requests(self):
        self.start_urls = [
            {"type": "apartment", "property_type": "apartment"},
	        {"type": "house", "property_type": "house"},
        ]
        self.index = 0
        self.headers = {
            "Accept": "*/*",
            "Origin": "https://parkerwilliams.nl",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
        }
        
        self.data = {
            "search": "W10=",
            "page": "1",
            "type": "rent",
            "propertytype": self.start_urls[self.index]["type"], 
            "type": "rent",
            "propertyresultsort": "askingpriceasc",
        }

        yield FormRequest(
            "https://parkerwilliams.nl/en/smarthousing/search",
            formdata=self.data,
            headers=self.headers,
            callback=self.parse,
            meta={'property_type': self.start_urls[self.index].get('property_type')
        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        jresp = json.loads(response.body)
        
        for item in jresp["results"].values():
            follow_url = response.urljoin(item["link"])
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type, "item":item})
        
        if jresp["paging"]["currentPage"] != jresp["paging"]["lastPage"]:
            self.data["page"] = str(page)
            yield FormRequest(
                "https://parkerwilliams.nl/en/smarthousing/search",
                formdata=self.data,
                headers=self.headers,
                dont_filter=True,
                callback=self.parse,
                meta={'property_type': self.start_urls[self.index].get('property_type'),
                    'page':page+1
            })
        else:
            if self.index == 0:
                self.index = 1
                self.data["page"] = "1"
                self.data["propertytype"] = self.start_urls[self.index]["type"],
                yield FormRequest(
                    "https://parkerwilliams.nl/en/smarthousing/search",
                    formdata=self.data,
                    headers=self.headers,
                    dont_filter=True,
                    callback=self.parse,
                    meta={'property_type': self.start_urls[self.index].get('property_type'),
                        'page':2
                })
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Parkerwilliams_PySpider_netherlands")
        title = response.xpath("//div[contains(@class,'property-header')]/h2/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        address =", ".join(response.xpath("//div[contains(@class,'property-header')]/h2/text() | //div[contains(@class,'property-header')]/div[contains(@class,'address')]/text()").getall())
        if address:
            item_loader.add_value("address", address.replace("  ","").strip())
        jresp = response.meta.get("item")
    
        lat = jresp["coordinates"]["coordinates"][0]
        lng = jresp["coordinates"]["coordinates"][1]
        item_loader.add_value("latitude", str(lat))
        item_loader.add_value("longitude", str(lng))
        item_loader.add_value("city", jresp["city"])
        item_loader.add_value("deposit", jresp["deposit"])
        item_loader.add_value("zipcode", jresp["zip_code"])
        item_loader.add_value("rent", jresp["rent_price"])
        item_loader.add_value("currency", "EUR")
        item_loader.add_value("utilities", jresp["service_costs"])
        if "rooms" in jresp:
            if "countOfBedrooms" in jresp["rooms"]:
                item_loader.add_value("room_count", jresp["rooms"]["countOfBedrooms"])
            elif "countOfRooms" in jresp["rooms"]:
                item_loader.add_value("room_count", jresp["rooms"]["countOfRooms"])
            if "countOfBathrooms" in jresp["rooms"]:
                item_loader.add_value("bathroom_count", jresp["rooms"]["countOfBathrooms"])
            if "countOfFloors" in jresp["rooms"]:
                item_loader.add_value("floor", str(jresp["rooms"]["countOfFloors"]))
      
        if "energy_label" in jresp:
            item_loader.add_value("energy_label", jresp["energy_label"])
        item_loader.add_value("square_meters", jresp["measurements"]["effectiveArea"])

        if "furniture" in jresp:
            furnished = jresp["furniture"]
            if "true" in furnished:
                item_loader.add_value("furnished", True)
            elif "false" in furnished:
                item_loader.add_value("furnished", False)
            balcony = jresp["balcony"]
            if "true" in balcony:
                item_loader.add_value("balcony", True)
            elif "false" in balcony:
                item_loader.add_value("balcony", False)
            elevator = jresp["elevator"]
            if "true" in elevator:
                item_loader.add_value("elevator", True)
            elif "false" in elevator:
                item_loader.add_value("elevator", False)    
            terrace = jresp["roof_terrace"]
            if "true" in terrace:
                item_loader.add_value("terrace", True)
            elif "false" in terrace:
                item_loader.add_value("terrace", False)  

        images = [response.urljoin(x) for x in response.xpath("//div[@id='housegridCarousel']/div//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        desc = " ".join(response.xpath("//div[contains(@class,'fade-content-desc')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.replace("         Read full description","").strip())
       
        item_loader.add_value("landlord_name", "Parker & Williams")
        item_loader.add_value("landlord_phone", "+31 20 - 675 02 02")
        item_loader.add_value("landlord_email", "info@parkerwilliams.nl")
        

        yield item_loader.load_item()