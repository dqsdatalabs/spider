# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'bps_london'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    post_url = ['https://www.bps.london/properties?eapowquicksearch=1&limitstart=0']  # LEVEL 1
    current_index = 0
    other_prop = ["4"]
    other_prop_type = ["house"]
    def start_requests(self):
        form_data = {
            "filter_cat": "2",
            "tx_placename": "",
            "filter_rad": "5",
            "eapow-qsmod-types": "5",
            "selectItemeapow-qsmod-types": "5",
            "filter_keyword": "",
            "filter_beds": "",
            "filter_price_low": "",
            "filter_price_high": "",
            "commit": "",
            "filter_lat": "0",
            "filter_lon": "0",
            "filter_location": "[object Object]",
            "filter_types": "5",
        }
        yield FormRequest(
            url=self.post_url[0],
            callback=self.parse,
            dont_filter=True,
            formdata=form_data,
            meta={
                "property_type":"apartment",
            }
        )

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@id='smallProps']/div[contains(@class,'eapow-overview-row')]"):
            let_stc = item.xpath(".//div[@class='eapow-bannertopright']/img[@alt='Let STC']/@alt").get()
            url = item.xpath(".//a[.='Read more...']/@href").get()
            if let_stc:
                continue
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
        
        next_page = response.xpath("//li/a[.='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})
        elif self.current_index < len(self.other_prop):
            form_data = {
                "filter_cat": "2",
                "tx_placename": "",
                "filter_rad": "5",
                "eapow-qsmod-types": self.other_prop[self.current_index],
                "selectItemeapow-qsmod-types": self.other_prop[self.current_index],
                "filter_keyword": "",
                "filter_beds": "",
                "filter_price_low": "",
                "filter_price_high": "",
                "commit": "",
                "filter_lat": "0",
                "filter_lon": "0",
                "filter_location": "[object Object]",
                "filter_types": self.other_prop[self.current_index],
            }
            yield FormRequest(
                url=self.post_url[0],
                callback=self.parse,
                dont_filter=True,
                formdata=form_data,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        if "property" in response.url:
            item_loader.add_value("external_link", response.url)
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_source", "Bps_London_PySpider_united_kingdom")
            
            external_id = response.xpath("//b[contains(.,'Ref')]//parent::div/text()").get()
            if external_id:
                external_id = external_id.replace(":","").strip()
                item_loader.add_value("external_id", external_id)

            title = " ".join(response.xpath("//h1//text()").getall())
            if title:
                title = re.sub('\s{2,}', ' ', title.strip())
                item_loader.add_value("title", title)

            address = "".join(response.xpath("//div[contains(@class,'mainaddress')]//text()").getall())
            if address:
                item_loader.add_value("address", address.strip())

            city_zipcode = response.xpath("//div[contains(@class,'mainaddress')]//address/text()").get()
            if city_zipcode:
                city = city_zipcode.strip().split(" ")[0]
                zipcode = city_zipcode.split(city)[1].strip()
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)

            rent = response.xpath("//small[contains(@class,'price')]//text()").get()
            if rent:
                rent = rent.split("£")[1].replace(",","")
                item_loader.add_value("rent", int(rent)*4)
            item_loader.add_value("currency", "GBP")

            desc = " ".join(response.xpath("//ul[contains(@id,'starItem')]//following-sibling::p//text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)
            else:
                desc = " ".join(response.xpath("//div[contains(@class,'tab-content')]//following-sibling::div//div[contains(@class,'span12')]//text()").getall())
                if desc:
                    desc = re.sub('\s{2,}', ' ', desc.strip())
                    item_loader.add_value("description", desc)

            room_count = response.xpath("//i[contains(@class,'propertyIcons-bedrooms')]//following-sibling::strong//text()").get()
            if room_count and room_count >"0":
                room_count = room_count.strip()
                item_loader.add_value("room_count", room_count)

            bathroom_count = response.xpath("//i[contains(@class,'propertyIcons-bathrooms')]//following-sibling::strong//text()").get()
            if bathroom_count:
                bathroom_count = bathroom_count.strip()
                item_loader.add_value("bathroom_count", bathroom_count)
            
            images = [x for x in response.xpath('//ul[@class="slides"]//@src').getall()]
            if images:
                item_loader.add_value("images", images)
            
            floor_plan_images = response.xpath("//div[contains(@id,'eapowfloorplanplug')]//@src").get()
            if floor_plan_images:
                item_loader.add_value("floor_plan_images", floor_plan_images)
            
            from datetime import datetime
            import dateparser
            available_date = "".join(response.xpath("//ul[contains(@id,'starItem')]//li[contains(.,'Available')]//text()").getall())
            if available_date:
                available_date = available_date.split("Available")[1].replace("late","").replace("early","").replace("!","")
                if not "now" in available_date.lower():
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)

            parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking')]//text()").get()
            if parking:
                item_loader.add_value("parking", True)

            balcony = response.xpath("//ul[contains(@id,'starItem')]//li[contains(.,'balcon') or contains(.,'Balcon')]//text()").get()
            if balcony:
                item_loader.add_value("balcony", True)
            
            terrace = response.xpath("//ul[contains(@id,'starItem')]//li[contains(.,'terrace')]//text()").get()
            if terrace:
                item_loader.add_value("terrace", True)

            furnished = response.xpath("//li[contains(.,'furnished') or contains(.,'Furnished')]//text()[not(contains(.,'Unfurnished') or contains(.,'unfurnished'))]").get()
            if furnished:
                item_loader.add_value("furnished", True)

            elevator = response.xpath("//ul[contains(@id,'starItem')]//li[contains(.,'Lift') or contains(.,'lift')]//text()").get()
            if elevator:
                item_loader.add_value("elevator", True)

            floor = response.xpath("//ul[contains(@id,'starItem')]//li[contains(.,' floor')]//text()[not(contains(.,'floors') or contains(.,'flooring'))]").get()
            if floor:
                floor = floor.strip().split(" ")[0]
                item_loader.add_value("floor", floor.strip())
                
            swimming_pool = response.xpath("//li[contains(.,'Pool')]//text()").get()
            if swimming_pool:
                item_loader.add_value("swimming_pool", True)
                
            dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
            if dishwasher:
                item_loader.add_value("dishwasher", True)
            
            washing_machine = response.xpath("//li[contains(.,'Washer') or contains(.,'dryer')]//text()").get()
            if washing_machine:
                item_loader.add_value("washing_machine", True)

            latitude_longitude = response.xpath("//script[contains(.,'lat:')]//text()").get()
            if latitude_longitude:
                latitude = latitude_longitude.split('lat: "')[1].split('"')[0]
                longitude = latitude_longitude.split('lon: "')[1].split('"')[0].strip()      
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

            landlord_name = response.xpath("//div[contains(@class,'agentBox')]//b//text()").get()
            if landlord_name:
                item_loader.add_value("landlord_name", landlord_name)

            item_loader.add_value("landlord_email", "info@bps.london")
            
            landlord_phone = response.xpath("//div[contains(@class,'agentBox')]//div[contains(@class,'phone')]//text()").get()
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone.strip())
    
            yield item_loader.load_item()