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
    name = 'living_london'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    prop_type = [
                    {"prop_type":"apartment", "type":"5"},
                    {"prop_type":"apartment", "type":"6"},
                    {"prop_type":"apartment", "type":"9"},
                    {"prop_type":"house", "type":"4"},
                    {"prop_type":"house", "type":"3"},
                    {"prop_type":"room", "type":"10"},
                    {"prop_type":"room", "type":"11"},
    ]

    headers = {
            "content-type": "application/x-www-form-urlencoded",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "origin": "https://www.living.london"
    }
    def start_requests(self):

        data = {
            "filter_cat": "2",
            "tx_placename": "",
            "filter_rad": "5",
            "filter_keyword": "",
            "filter_beds": "",
            "eapow-qsmod-types": self.prop_type[0]["type"],
            "selectItemeapow-qsmod-types": self.prop_type[0]["type"],
            "filter_price_low": "",
            "filter_price_high": "",
            "commit": "",
            "filter_lat": "0",
            "filter_lon": "0",
            "filter_location": "[object Object]",
            "filter_types": self.prop_type[0]["type"],
        }
    
        url = "https://www.living.london/properties?eapowquicksearch=1&limitstart=0"        
        yield FormRequest(
            url,
            formdata=data,
            headers=self.headers,
            dont_filter=True,
            callback=self.jump,
            meta={"property_type":self.prop_type[0]["prop_type"], "first":True, "count":0},
        )
    
    def jump(self, response):
        if response.meta.get("first"):
            url = "https://www.living.london/properties?eapowquicksearch=1&limitstart=0"
            yield Request(url, callback=self.parse, 
                        dont_filter=True, 
                        meta={"property_type":response.meta.get("property_type"),
                                "count":response.meta.get("count")
                        })
        else:
            count = response.meta.get("count")
            data = {
                "filter_cat": "2",
                "tx_placename": "",
                "filter_rad": "5",
                "filter_keyword": "",
                "filter_beds": "",
                "eapow-qsmod-types": self.prop_type[count]["type"],
                "selectItemeapow-qsmod-types": self.prop_type[count]["type"],
                "filter_price_low": "",
                "filter_price_high": "",
                "commit": "",
                "filter_lat": "0",
                "filter_lon": "0",
                "filter_location": "[object Object]",
                "filter_types": self.prop_type[count]["type"],
            }
            # print("/////////////////////// TYPE " + self.prop_type[count]["type"])
            url = "https://www.living.london/properties?eapowquicksearch=1&limitstart=0"        
            yield FormRequest(
                url,
                formdata=data,
                headers=self.headers,
                dont_filter=True,
                callback=self.jump,
                meta={"property_type":self.prop_type[count]["prop_type"], "count":count, "first":True}, 
            )

    def parse(self, response):
        property_type = response.meta.get("property_type")
 
        for item in response.xpath("//div[@id='smallProps']/div[not(.//div[@class='eapow-bannertopright']) and contains(@class,'row-fluid')]//h3/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//div[contains(@class,'span9')]//ul[@class='pagination-list']/li/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(response.urljoin(pagination), dont_filter=True, callback=self.parse, meta={"property_type":property_type,"count":response.meta.get("count")})
        else:
            url = "https://www.living.london/properties?eapowquicksearch=1&limitstart=0"
            count = response.meta.get("count") + 1
            # print(count)
            # print("////////////////////////////// COUNT ")
            if count < len(self.prop_type):
                yield Request(url, callback=self.jump, dont_filter=True, meta={"first":False,"count":count})
        
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Living_PySpider_united_kingdom")
        title = " ".join(response.xpath("//h1//text()").extract())
        if title:
            item_loader.add_value("title", title.strip())

        studio = response.xpath("//ul[@id='starItem']/li[contains(.,'Studio')]/text()").extract_first()
        if studio == "Studio":
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
    
        external_id = response.xpath("//div[@class='eapow-sidecol'][contains(.,'Ref #')]/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.replace(":","").strip())
            
        rent = response.xpath("//h1[contains(@class,'span8')]/small/text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.replace(",","."))
        latlng=response.xpath("//script[contains(.,'lon')]").get() 
        if latlng:
            lat=latlng.split("lat:")[-1].split(",")[0].replace("\\","").replace('"',"")
            lng=latlng.split("lon:")[-1].split(",")[0].replace("\\","").replace('"',"")
            if lat:
                item_loader.add_value("latitude",lat)
            if lng:
                item_loader.add_value("longitude",lng)

        room = response.xpath("//div[@class='span12']/i[@class='flaticon-bed']/following-sibling::strong[1]/text()").extract_first()
        if room:
            room2 = response.xpath("//div[@class='span12']/i[@class='flaticon-sofa']/following-sibling::strong[1]/text()").extract_first()
            if room2:
                item_loader.add_value("room_count", int(room)+int(room2))
            else:
                item_loader.add_value("room_count", room)

        bathroom_count = response.xpath("//div[@class='span12']/i[@class='flaticon-bath']/following-sibling::strong[1]/text()").extract_first()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        floor = response.xpath("//ul[@id='starItem']/li[contains(.,'Floor')]/text()").extract_first()
        if floor:
            if "floor" in floor:
                item_loader.add_value("floor", floor.split("floor")[0])
            if "Floor" in floor:
                item_loader.add_value("floor", floor.split("Floor")[0])
        address = " ".join(response.xpath("//h1[contains(@class,'span8')]/text()").extract())
        if address:           
            city = address.strip().split(",")[1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city.strip())

        available_date=response.xpath("//ul[@id='starItem']/li[contains(.,'Available from')]/text()").get()
        if available_date:
            date2 =  available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        desc = "".join(response.xpath("//div[contains(@class,'span12')]/div/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [
            response.urljoin(x)
            for x in response.xpath("//div[@class='eapow-galleryplug-img pull-left thumbnail']/div/a/@href").extract()
        ]
        item_loader.add_value("images", images)

        floor_plan_images = [
            response.urljoin(x)
            for x in response.xpath("//div[@class='eapow-galleryplug-img pull-left thumbnail']/div/a/@href[(contains(.,'Floorplan'))]").extract()
        ]
        item_loader.add_value("floor_plan_images", floor_plan_images)

        balcony = " ".join(response.xpath("//ul[@id='starItem']/li[contains(.,'balcony') or contains(.,'Balcony')]/text()").extract())
        if balcony:
            item_loader.add_value("balcony", True) 

        parking = " ".join(response.xpath("//ul[@id='starItem']/li[contains(.,'Parking') or contains(.,'parking')]/text()").extract())
        if parking:
            item_loader.add_value("parking", True) 

        elevator = " ".join(response.xpath("//ul[@id='starItem']/li[contains(.,'Lift') or contains(.,'lift')]/text()").extract())
        if elevator:
            item_loader.add_value("elevator", True) 
        
        land_name = response.xpath("//div[@class='span8']/a/b/text()").get()
        if land_name:
            item_loader.add_value("landlord_name", land_name)

            email = land_name.split(" ")[0].strip().lower() + "@living.london"
            item_loader.add_value("landlord_email", email.strip())

        phone = "".join(response.xpath("//div[contains(@class,'phone')]/text()").getall())
        if phone:
            item_loader.add_value("landlord_phone", phone.strip())

        zipcode = "".join(response.xpath("//div[contains(@class,'eapow-mainaddress')]/address/text()").getall())
        if zipcode:
            zipcode = " ".join(zipcode.split(" ")[1:])
            item_loader.add_value("zipcode", zipcode)
       

        yield item_loader.load_item()