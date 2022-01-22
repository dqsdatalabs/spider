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
    name = 'dutchhousing_nl'
    start_urls = ['https://www.dutchhousing.nl/nl/realtime-listings/consumer'] 
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        data =json.loads(response.body)
        
        for item in data:
            if item["isRentals"]:
                property_type = item.get('mainType')
                if "apartment" in property_type:
                    propperty_type = "apartment"
                elif "house" in property_type:
                    propperty_type = "house"
                else:
                    propperty_type = None
                follow_url = response.urljoin(item["url"])
                lat = item["lat"]
                lng = item["lng"]
                
                yield Request(follow_url, callback=self.populate_item, meta={"lat": lat, "lng": lng, "property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Dutchhousingcentre_PySpider_" + self.country + "_" + self.locale)
        
        verhuud = response.xpath("//dl[@class='full-details']/dt[.='Status']/following-sibling::dd[1]/text()").extract_first()
        if verhuud != "Verhuurd":
            title = response.xpath("normalize-space(//h1/text())").extract_first()
            item_loader.add_value("title", title)
            item_loader.add_value("external_link", response.url)
            property_type = response.meta.get('property_type')
            if property_type:
                item_loader.add_value("property_type", property_type)
            else:
                return

            item_loader.add_value("external_id", response.url.split('/')[-1])

            desc = "".join(response.xpath("//p[@class='object-description']/text()").extract())
            item_loader.add_value("description", desc.strip())

            price = response.xpath("normalize-space(//h2/text()[contains(.,'€')])").get()
            if price:
                item_loader.add_value(
                    "rent", price.split(" ")[1])
                item_loader.add_value("currency", "EUR")

            square = response.xpath(
                "normalize-space(//dl[@class='full-details']/dt[.='Woonoppervlakte']/following-sibling::dd[1]/text())"
            ).get()
            if square:
                item_loader.add_value(
                    "square_meters", square.split("m²")[0]
                )
            room_count = response.xpath(
                "normalize-space(//dl[@class='full-details']/dt[.='Aantal slaapkamers']/following-sibling::dd[1]/text())"
            ).get()
            if room_count:
                item_loader.add_value("room_count", room_count)

            address1 = response.xpath("//div[@class='simple-map-markers']/text()").get()
            if address1:
                zipcode = address1.split('"zipCode":')[1].split(",")[0].replace('"','').strip()
                city = address1.split('"city":')[1].split(",")[0].replace('"','').strip()
                address = address1.split('"address":')[1].split(",")[0].replace('"','').strip()
                item_loader.add_value("address", address + " " + city)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
            
            item_loader.add_xpath("bathroom_count", "//dl[@class='full-details']/dt[.='Aantal badkamers']/following-sibling::dd[1]/text()")
            item_loader.add_xpath("utilities", "substring-after(//dl[@class='full-details']/dt[.='Servicekosten']/following-sibling::dd[1]/text(),'€ ')")
                
            available_date = response.xpath(
                "//dl[@class='full-details']/dt[.='Oplevering']/following-sibling::dd[1]/text()[.!='Per direct' and .!='In overleg']").get()
            if available_date:
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%m-%d-%Y"]
                )
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

            item_loader.add_xpath(
                "floor",
                "normalize-space(//dl[@class='full-details']/dt[.='Aantal verdiepingen']/following-sibling::dd[1]/text())"
            )


            furnished = response.xpath(
                "normalize-space(//dl[@class='full-details']/dt[.='Inrichting']/following-sibling::dd[1]/text()[.='Gestoffeerd' or .='Gemeubileerd'])"
            ).get()
            if furnished:
                item_loader.add_value("furnished", True)
            
            parking = response.xpath(
                "//dl[@class='full-details']/dt[.='Soort garage']/following-sibling::dd[1]/text()").get()
            if parking:
                item_loader.add_value("parking", True)
            elif response.xpath("//h4[contains(.,'Garage')]").get(): item_loader.add_value("parking", True)

            elevator = response.xpath(
                "//dl[@class='full-details']/dt[.='Voorzieningen']/following-sibling::dd[1]/text()[contains(.,'Lift')]").get()
            if elevator:
                item_loader.add_value("elevator", True)
            
            item_loader.add_xpath("energy_label", "normalize-space(//dl[@class='full-details']/dt[.='Energielabel']/following-sibling::dd[1]/text())")

            images = [
                response.urljoin(x)
                for x in response.xpath(
                    "//div[@class='photo-slider-slides']/img/@src"
                ).extract()
            ]
            if images:
                item_loader.add_value("images", images)
                  
            item_loader.add_value("landlord_phone", "020 - 662 1234")
            item_loader.add_value("landlord_name", "Dutch Housing Centre")
            item_loader.add_value("landlord_email", "info@dutchhousing.nl")

            
            item_loader.add_value("latitude", str(response.meta.get("lat")))
            item_loader.add_value("longitude", str(response.meta.get("lng")))

            yield item_loader.load_item()
def split_address(address, get):

    if "," in address:
        temp = address.split(",")[0]
        zip_code = "".join(filter(lambda i: i.isdigit(), temp))
        city = address.split(",")[1]

        if get == "zip":
            return zip_code
        else:
            return city