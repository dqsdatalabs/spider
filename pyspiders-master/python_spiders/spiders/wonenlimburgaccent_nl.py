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
    name = 'wonenlimburgaccent_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'

    s = {}
    
    def start_requests(self):
        headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "origin": "https://www.wonenlimburgaccent.nl"
        }
       
        url = "https://www.wonenlimburgaccent.nl/portal/object/frontend/getallobjects/format/json"
        yield Request(url, self.parse, method="POST", headers=headers)
    
    # 1. FOLLOWING
    def parse(self, response):
        
        data = json.loads(response.body)
        
        headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36",
            "origin": "https://www.wonenlimburgaccent.nl"
        }
        
        for item in data["result"]:
            follow_url = "https://www.wonenlimburgaccent.nl/te-huur/details/" + item["id"]
            # follow_url = "https://www.wonenlimburgaccent.nl/te-huur/details/260"
            
            data = {"id":f"{item['id']}"}
            
            url = "https://www.wonenlimburgaccent.nl/portal/object/frontend/getobject/format/json"
            yield FormRequest(
                url,
                formdata=data,
                headers=headers,
                callback=self.populate_item,
                meta={"url":follow_url}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Wonenlimburgaccent_PySpider_" + self.country + "_" + self.locale)
        
        data = json.loads(response.body)

        item_loader.add_value("title", data["result"]["street"] + " " + data["result"]["houseNumber"])
        item_loader.add_value("external_link", response.meta.get("url"))

        prop_type = data["result"]["dwellingType"]["categorie"]
        if prop_type and "woning" in prop_type:
            item_loader.add_value("property_type", "house")
        elif prop_type and "appartement" in prop_type:
            item_loader.add_value("property_type", "apartment")
        else:
            return
        
        item_loader.add_value("description", data["result"]["description"])
        # rent =  response.xpath("//div[@class='price']/div/span[@class='amount']/text()").extract_first()
        # if rent:
        #     price = rent.split("€")[1].strip()
        #     item_loader.add_value("rent", int(float(price)))
        # else:
        item_loader.add_value("rent", str(int(float(data["result"]["totalRent"]))))
        item_loader.add_value("utilities", int(float(data["result"]["serviceCosts"])))
        item_loader.add_value("currency", "EUR")
        square = data["result"]["areaLivingRoom"]
        if square:
            item_loader.add_value("square_meters", square)
        elif not square:
            square = data["result"]["areaDwelling"]
            if square:
                item_loader.add_value("square_meters",square)
                
        item_loader.add_value("room_count", str(data["result"]["sleepingRoom"]["amountOfRooms"]))

        external_id = data["result"]["id"]
        if external_id:
            item_loader.add_value("external_id", external_id)

        available_date = data["result"]["availableFromDate"]
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%B/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        floor_plan_images = [response.urljoin(x["uri"]) for x in data["result"]["floorplans"]]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        if data["result"]["description"]:
            if 'wasmachine' in data["result"]["description"]:
                item_loader.add_value("washing_machine", True)

        street = data["result"]["street"]
        city = data["result"]["city"]["name"]
        zipcode = data["result"]["postalcode"]
        item_loader.add_value("address", street + " " + zipcode + " " + city)
        item_loader.add_value("zipcode", zipcode)
        item_loader.add_value("city", city)
            
        
        floor = data["result"]["floor"]["localizedName"]
        if floor:
            item_loader.add_value("floor", floor.split("e verdieping")[0].replace("Begane ","").strip())

        furnished = response.xpath(
            "//div[@class='interior']/small[contains(.,'Furnished')]"
        ).get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        
        balcony = data["result"]["balcony"]
        if balcony:
            if balcony == True:
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)

        energy_label = data["result"]["energyLabel"]["localizedName"]
        if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
            item_loader.add_value("energy_label", energy_label)

        
        images = [response.urljoin(item["uri"])
                    for item in data["result"]["pictures"]
                ]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))

        
        item_loader.add_value("landlord_phone", "+31 (0)10 41 22 221")
        item_loader.add_value("landlord_name", "Wonen Limburg Accent")
        item_loader.add_value("landlord_email", "info@rotterdamapartments.com")

        item_loader.add_value("latitude", str(data["result"]["latitude"]))
        item_loader.add_value("longitude", str(data["result"]["longitude"]))

        yield item_loader.load_item()