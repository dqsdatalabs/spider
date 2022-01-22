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
    name = 'kingsandmarsh_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "5,6,10"
            },
            {
                "property_type" : "house",
                "type" : "3,4,8"
            },
        ]
        for item in start_urls:
            formdata = {
                "filter_cat": "2",
                "tx_placename": "",
                "filter_rad": "5",
                "eapow-qsmod-types": item["type"].split(",")[0],
                "eapow-qsmod-types": item["type"].split(",")[1],
                "eapow-qsmod-types": item["type"].split(",")[2],
                "selectItemeapow-qsmod-types": item["type"].split(",")[0],
                "selectItemeapow-qsmod-types": item["type"].split(",")[1],
                "selectItemeapow-qsmod-types": item["type"].split(",")[2],
                "filter_keyword": "",
                "filter_beds": "",
                "filter_baths": "",
                "filter_price_low": "",
                "filter_price_high": "",
                "commit": "",
                "filter_lat": "0",
                "filter_lon": "0",
                "filter_location": "[object Object]",
                "filter_types": item["type"],
            }
            yield FormRequest(
                "http://www.kingsandmarsh.com/properties?eapowquicksearch=1&limitstart=0",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 12)
        seen = False
        for item in response.xpath("//div[@class='eapow-property-thumb-holder']"):
            status = item.xpath("./div/img/@alt").get()
            if status and ("under" in status.lower() or "let stc" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 12 or seen:
            p_type = response.meta["type"]
            formdata = {
                "filter_cat": "2",
                "tx_placename": "",
                "filter_rad": "5",
                "eapow-qsmod-types": p_type.split(",")[0],
                "eapow-qsmod-types": p_type.split(",")[1],
                "eapow-qsmod-types": p_type.split(",")[2],
                "selectItemeapow-qsmod-types": p_type.split(",")[0],
                "selectItemeapow-qsmod-types": p_type.split(",")[1],
                "selectItemeapow-qsmod-types": p_type.split(",")[2],
                "filter_keyword": "",
                "filter_beds": "",
                "filter_baths": "",
                "filter_price_low": "",
                "filter_price_high": "",
                "commit": "",
                "filter_lat": "0",
                "filter_lon": "0",
                "filter_location": "[object Object]",
                "filter_types": p_type,
            }
            p_url = f"http://www.kingsandmarsh.com/properties?eapowquicksearch=1&limitstart={page}"
            yield FormRequest(
                p_url,
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":response.meta["property_type"],
                    "page":page+12,
                    "type":p_type,
                })

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])

        item_loader.add_value("external_source", "Kingsandmarsh_PySpider_united_kingdom")

        external_id = response.xpath("//b[contains(.,'Ref')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())

        address = " ".join(response.xpath("//div[@id='propdescription']//address//text()").getall()).strip()
        if address:
            item_loader.add_value("address", address.strip())
        
        city_zipcode = response.xpath("//div[@id='propdescription']//address/br/following-sibling::text()").get()
        if city_zipcode:
            item_loader.add_value("zipcode", " ".join(city_zipcode.strip().split(' ')[1:]))
            item_loader.add_value("city", city_zipcode.strip().split(' ')[0].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[contains(@class,'desc')]//*[self::p or self::div]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//li[contains(.,'sq ') or contains(.,'Sq ')]/text()").get()
        if square_meters:
            if 'sq m' in square_meters.lower():
                item_loader.add_value("square_meters", "".join(filter(str.isnumeric, square_meters.split('sq ')[0].strip())))
            elif 'sq ft' in square_meters.lower():
                square_meters = "".join(filter(str.isnumeric, square_meters.split('sq ')[0].strip()))
                if square_meters:
                    item_loader.add_value("square_meters", str(int(float(square_meters) * 0.09290304)))

        room_count = response.xpath("//img[contains(@src,'bedroom')]/following-sibling::strong[1]/text()").get()
        if room_count:
            if room_count != "0":
                item_loader.add_value("room_count", room_count.strip())
            else:
                room_count = response.xpath("//img[contains(@src,'receptions')]/following-sibling::strong[1]/text()").get()
                if room_count:
                    item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//img[contains(@src,'bathroom')]/following-sibling::strong[1]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//small[contains(@class,'price')]/text()").get()
        if rent:
            try:
                rent = rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", rent)
                item_loader.add_value("currency", 'GBP')
            except: pass

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//li[contains(.,'Available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().split('available')[-1].split('from')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['en'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='eapowgalleryplug']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='eapowfloorplanplug']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'lat:')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('lat: "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latitude.split('lon: "')[1].split('"')[0].strip())

        energy_label = response.xpath("//text()[contains(.,'EPC rating')]").get()
        if energy_label:
            energy_label = energy_label.split('EPC rating')[1].split('=')[-1].strip().split(' ')[0].strip().strip('.')
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)
        
        floor = response.xpath("//li[contains(.,'Floor')]").get()
        if floor:
            floor = "".join(filter(str.isnumeric, floor.lower().split('floor')[0].strip()))
            if floor:
                item_loader.add_value("floor", floor)
        
        parking = response.xpath("//li[contains(.,'Parking')] | //ul[@style='list-style-type: disc;']/li/span/text()[contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony') or contains(.,'BALCONY')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Unfurnished') or contains(.,'unfurnished')]").get()
        if furnished:
            item_loader.add_value("furnished", False)
        else:
            furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')] | //p/span[contains(.,' furnished') or contains(.,'Furnished')]/text()").get()
            if furnished:
                item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        dishwasher = response.xpath("//li[contains(.,'dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        item_loader.add_value("landlord_name", "Kings & Marsh")
        item_loader.add_value("landlord_phone", "0207 720 1000 ")
        item_loader.add_value("landlord_email", "info@kingsandmarsh.com")

        yield item_loader.load_item()
