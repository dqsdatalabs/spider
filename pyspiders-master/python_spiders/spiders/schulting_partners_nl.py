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
    name = 'schulting_partners_nl'
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    current_index = 0
    other_prop = ["Beneden/bovenwoningen","Eengezinswoningen"]
    other_prop_type = ["apartment","house"]
    # 1. FOLLOWING
    def start_requests(self): 
        formdata = {
            "huistype": "Appartementen/flats",
            "sorteer": "ASC~StatusId|Desc~Datum",
            "prijs": "0,999999999",
            "prefilter": "Huuraanbod",
            "pagenum": "0",
            "pagerows": "12",
        }
        yield FormRequest(
            "https://www.schulting-partners.nl/huizen/smartselect.aspx",
            callback=self.jump,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type":"apartment",
                "type" : "Appartementen/flats"
            })
    
    def jump(self, response):
        data = json.loads(response.body)
        id_list = ""
        for i in data["AllMatches"]:
            id_list = id_list + i + ","
        
        formdata = {
            "id" : id_list.strip(",").strip(),
        }
        yield FormRequest(
            "https://www.schulting-partners.nl/huizen/smartelement.aspx",
            callback=self.parse,
            formdata=formdata,
            dont_filter=True,
            meta={
                "property_type": response.meta["property_type"],
                "type" : response.meta["type"],
            })
    

    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//a[contains(@class,'adreslink')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            p_type = response.meta["type"]
            formdata = {
                "huistype": p_type,
                "sorteer": "ASC~StatusId|Desc~Datum",
                "prijs": "0,999999999",
                "prefilter": "Huuraanbod",
                "pagenum": str(page),
                "pagerows": "12",
            }
            yield FormRequest(
                "https://www.schulting-partners.nl/huizen/smartselect.aspx",
                callback=self.jump,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":"apartment",
                    "type" : p_type,
                })
        if self.current_index < len(self.other_prop_type):
            formdata = {
                "huistype": self.other_prop[self.current_index],
                "sorteer": "ASC~StatusId|Desc~Datum",
                "prijs": "0,999999999",
                "prefilter": "Huuraanbod",
                "pagenum": "0",
                "pagerows": "12",
            }
            yield FormRequest(
                "https://www.schulting-partners.nl/huizen/smartselect.aspx",
                callback=self.jump,
                formdata=formdata,
                dont_filter=True,
                meta={
                    "property_type":self.other_prop_type[self.current_index],
                    "type" : self.other_prop[self.current_index],
                   
                })
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        if response.xpath("//div[contains(@class,'status-rent')]/span/text()[contains(.,'Verhuurd') or contains(.,'Onder optie')]").get(): return

        property_type = response.xpath("//div[contains(@class,'features-info')][contains(.,'Studio')]//text()").get()
        if property_type:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_source", "Schulting_Partners_PySpider_netherlands")

        external_id = response.url.split('-')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//h1/span[@class='adres']/text()").get()
        city = response.xpath("//h1/span[@class='plaatsnaam']/text()").get()
        if city:      
            item_loader.add_value("city", city.strip())
            if address: address += " " + city
        
        if address:
            item_loader.add_value("address", address.strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@id='object-description']/div[contains(@class,'small')]//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//div[contains(text(),'Gebruiksoppervlak wonen')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(',')[0].split('.')[0].strip())

        room_count = response.xpath("//div[contains(text(),'Aantal slaapkamers')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[contains(text(),'Aantal badkamers')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        rent = response.xpath("//h2/text()").get()
        if rent:
            rent = rent.split('€')[-1].lower().split('p')[0].strip().replace('.', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//div[contains(text(),'Status')]/following-sibling::div/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.lower().replace('beschikbaar', 'nu').strip(), date_formats=["%d/%m/%Y"], languages=['nl'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//div[contains(text(),'Waarborgsom')]/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[-1].strip().replace('.', ''))
        
        images = [response.urljoin(x) for x in response.xpath("//section[@id='object-all-photos']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//section[@id='object-all-photos']//img[contains(@src,'Plattegrond')]/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//input[contains(@id,'mgmMarker')]/@value").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('~')[2].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('~')[2].split(',')[1].strip())
        
        energy_label = response.xpath("//div[contains(text(),'Energielabel')]/following-sibling::div/span/text()").get()
        if energy_label:
            if energy_label.lower().split('klasse')[-1].strip().upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label.lower().split('klasse')[-1].strip().upper())
        
        parking = response.xpath("//div[contains(text(),'Soort parkeergelegenheid')]/following-sibling::div/text()").get()
        if parking:
            if 'parkeren' in parking.strip().lower():
                item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Schulting & Partners LLP")
        item_loader.add_value("landlord_phone", "0118-616617")
        item_loader.add_value("landlord_email", "info@schulting-partners.nl")
        
        status = response.xpath("//div[contains(@class,'features-info')][contains(.,'Verhuurd')]//text()").get()
        if not status:
            yield item_loader.load_item()
