# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re
import dateparser

class MySpider(Spider):
    name = 'op_be'
    execution_type='testing'
    country='france'
    locale='fr'

    headers = {
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
            "origin": "https://op.be"
    }
    def start_requests(self):
        
        prop_types= [
            {"type": "2", "property_type": "apartment"},
	        {"type": "1", "property_type": "house"}
        ]
        for item in prop_types:
            data = {
                "page": "1",
                "function": "loadMoreToRentEstates",
                "clauses[ref]": "",
                "clauses[price_min]": "0",
                "clauses[price_max]": "10000",
                "clauses[area]": "",
                "clauses[category][]": item["type"],
                "clauses[rooms]": "",
                "clauses[city]": "",
                "clauses[order]": "",
                "clauses[funished]": "0",
                "clauses[get_page]": "reset",
                "clauses[clickedFlag]": "true",
                "nbItem": "0",
            }
        
            url = "https://op.be/fr/ajax"        
            yield FormRequest(
                url,
                formdata=data,
                headers=self.headers,
                dont_filter=True,
                callback=self.parse,
                meta={"property_type":item["property_type"], "data":data},
            )
    
    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)

        seen = False
        for follow_url in response.xpath("//div[contains(@class,'estates_count')]/div/a[@title='Détails']/@href").extract():
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})
            seen = True
        
        if page == 1 or seen:
            form_data = response.meta.get("data")
            form_data["page"] = str(page)

            count = response.meta.get('count', 9)
            form_data["nbItem"] = str(count)
        
            url = "https://op.be/fr/ajax"        
            yield FormRequest(
                url,
                formdata=form_data,
                headers=self.headers,
                dont_filter=True,
                callback=self.parse,
                meta={"property_type":property_type, "page":page+1, "count":count+9, "data":form_data},
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Op_PySpider_"+ self.country)
        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title)
            item_loader.add_value("title", title)
            
            address = title.split("- ")[-2]
            city = address.split(" ")[1].replace("-"," ")
            zipcode = address.split(" ")[0]
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            
            # rent = title.split("- ")[-1]
            # if rent and "€" in rent:
            #     price = rent.split("€")[0].strip().replace(".","")
            #     item_loader.add_value("rent", price)
        
        
        address = "".join(response.xpath("//div[@class='address']//text()").getall())
        if address:
            item_loader.add_value("address", address)
        
        rent = "".join(response.xpath("//ul/li[contains(.,'Prix :')]//span//text()").getall())
        if rent:
            item_loader.add_value("rent", rent.split("€")[0].strip().replace(".",""))
        item_loader.add_value("currency", "EUR")
        
        room_count = response.xpath("//div/img[contains(@alt,'bedroom')]/parent::div/text()").get()
        if room_count:
            room_count = room_count.strip()
            if room_count != "0":
                item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div/img[contains(@alt,'bathroom')]/parent::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        square_meters = response.xpath("//div/img[contains(@alt,'surface')]/parent::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.strip())
        
        energy_label = response.xpath("//div/img[contains(@src,'peb_')]/@alt").get()
        if energy_label and energy_label != "-":
            if energy_label == "bB":
                item_loader.add_value("energy_label", "B")
            else:
                item_loader.add_value("energy_label", energy_label.upper())
            
        external_id = "".join(response.xpath("//ul/li[contains(.,'Référence :')]//span//text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        parking = "".join(response.xpath("//ul/li[contains(.,'Parking')]//span//text()").getall())
        if parking:
            if "Oui" in parking:
                item_loader.add_value("parking", True)
            elif "Non" in parking:
                item_loader.add_value("parking", False)
        
        terrace = "".join(response.xpath("//ul/li[contains(.,'Terrasse')]//span//text()").getall())
        if terrace:
            if "Oui" in terrace:
                item_loader.add_value("terrace", True)
            elif "Non" in terrace:
                item_loader.add_value("terrace", False)
        
        available_date = "".join(response.xpath("//ul/li[contains(.,'Date')]//span//text()").getall())
        if available_date:
            item_loader.add_value("available_date", dateparser.parse(available_date, languages=['en']).strftime("%Y-%m-%d"))
        
        desc = "".join(response.xpath("//div[contains(@class,'item_details')]//p//text()").getall())
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc.strip()))
            if "charge" in desc.lower():
                utilities = desc.lower().split("charge")[1].split("€")[0]
                if utilities and utilities !='0':
                    item_loader.add_value("utilities", utilities)
        
        images = [ x for x in response.xpath("//div[contains(@class,'royalSlider')]//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        name = response.xpath("//div[@class='representative_infos']/div[contains(@class,'name')]/text()").get()
        if name:
            item_loader.add_value("landlord_name", name.strip())
        
        phone = response.xpath("//div[@class='representative_infos']/div[contains(.,'+')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.split("+")[1].strip())
            
        email = response.xpath("//div[@class='representative_infos']//a[@id='representative']//text()").get()
        if email:
            item_loader.add_value("landlord_email", email)
        
        yield item_loader.load_item()