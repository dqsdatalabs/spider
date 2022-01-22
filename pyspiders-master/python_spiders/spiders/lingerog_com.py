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
import dateparser

class MySpider(Spider):
    name = 'lingerog_com'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    # 1. FOLLOWING
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "benedenwoning"
            },
            {
                "property_type" : "apartment",
                "type" : "bovenwoning"
            },
            {
                "property_type" : "apartment",
                "type" : "galerijflat"
            },
            {
                "property_type" : "apartment",
                "type" : "portiekflat"
            },
            {
                "property_type" : "house",
                "type" : "portiekwoning"
            },
        ]
        for item in start_urls:
            formdata = {
                "__live": "1",
                "__templates[]": "search",
                "__templates[]": "loop",
                "__maps": "all",
                "plaats_postcode": "",
                "prijs": "",
                "orderby": "order:asc,publicatiedatum:asc",
                "status": "beschikbaar",
                "woningsoort": item["type"],
                "aantalKamers": "",
                "woonOppervlakte": "",
            }
            yield FormRequest(
                "https://www.lingerog.com/woningen/huur/",
                callback=self.parse,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":item["property_type"]
                }

            )
    

    def parse(self, response):
        data = json.loads(response.body)
        for item in data["maps"]:
            lat, lng = item["latitude"], item["longitude"]
            sel = Selector(text=item["template"], type="html")
            follow_url = response.urljoin(sel.xpath("//a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={
                    "property_type":response.meta["property_type"],
                    "lat" : lat, "lng" : lng,
                })
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("latitude", str(response.meta.get('lat')))
        item_loader.add_value("longitude", str(response.meta.get('lng')))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Lingerog_PySpider_netherlands")

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(',')[0].strip().split(' ')[-1].strip())
            item_loader.add_value("city", address.split(',')[-1].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@class='realworks--content-inner']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//dt[contains(.,'Woonopp')]/following-sibling::dd[1]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(',')[0].strip())

        room_count = response.xpath("//dt[contains(.,'Slaapkamers')]/following-sibling::dd[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        else:
            room_count = response.xpath("//dt[contains(.,'Kamers')]/following-sibling::dd[1]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        rent = response.xpath("//h1/..//div[contains(@class,'h1')]/text()").get()
        if rent:
            rent = rent.split('€')[-1].split(',')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//text()[contains(.,'beschikbaar') or contains(.,'Beschikbaar')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().replace('beschikbaar', '').replace('per', '').replace('direct', 'nu').strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = "".join(response.xpath("//text()[contains(.,'Waarborgsom') or contains(.,'waarborgsom') or contains(.,'month')]").getall())
        if deposit:
            if "maand" in deposit and "€" in deposit:
                item_loader.add_value("deposit", deposit.split("€")[1].strip().split(" ")[0].replace(",",""))
            elif "€" in deposit:
                item_loader.add_value("deposit", deposit.split("€")[1].strip().split(" ")[0].replace(",",""))
            elif "month" in deposit and "deposit" in deposit.lower():
                try:
                    deposit2 = deposit.split("month")[0].replace("-"," ").strip().split(" ")[-1]
                    item_loader.add_value("deposit", int(deposit2)*int(rent))
                except:
                    deposit2 = deposit.split("months")[0].replace("-"," ").strip().split(" ")[-1]
                    item_loader.add_value("deposit", int(deposit2)*int(rent))

            elif "maand" in deposit:
                deposit = deposit.split("maand")[0].strip().split(" ")[-1].replace("-","")
                if "twee" in deposit.lower():
                    deposit = 2
                item_loader.add_value("deposit", int(deposit)*int(rent))

        images = [response.urljoin(x) for x in response.xpath("//div[@id='images']//div[contains(@class,'images')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplans']//div[contains(@class,'js--realworks--floorplans')]//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('"latitude":')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('"longitude":')[1].split(',')[0].strip())

        utilities = response.xpath("//text()[contains(.,'servicekosten')]").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('servicekosten')[1].strip().split(' ')[0].split(',')[0].strip())
        
        parking = response.xpath("//text()[contains(.,'Parkeren') or contains(.,'parkeren') and contains(.,'–')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//text()[contains(.,'Balkon') or contains(.,'balkon') and contains(.,'–')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//text()[contains(.,'Gemeubileerd') or contains(.,'gemeubileerd') and contains(.,'–')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//text()[contains(.,'Lift') or contains(.,'lift') and contains(.,'–')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//text()[contains(.,'Dakterras') or contains(.,'dakterras') and contains(.,'–')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Linger OG")
        item_loader.add_value("landlord_phone", "020 623 63 77")
        item_loader.add_value("landlord_email", "info@lingerog.com")
        
        yield item_loader.load_item()
