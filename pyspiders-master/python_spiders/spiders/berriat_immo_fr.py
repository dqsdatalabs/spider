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

class MySpider(Spider):
    name = 'berriat_immo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Berriat_Immo_PySpider_france'
    start_urls = ['http://berriat-immo.fr/location-grenoble.php']  # LEVEL 1
    form_data = {
        "typebien": "Appartement",
        "nbpieces": "Indifferent",
        "prixmax": "",
        "zone": "",
        "image.x": "239",
        "image.y": "10",
    }
    current_index = 0
    other_type = ["Maison"]
    def start_requests(self):
        yield FormRequest(
            url=self.start_urls[0],
            callback=self.parse,
            dont_filter=True,
            formdata=self.form_data,
            meta={
                "property_type":"apartment",
                "type": "Appartement"
            }
        )

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 20)
        
        seen = False
        for item in response.xpath("//a/@onclick[contains(.,'details')]").extract():
            follow_url = response.urljoin(item.split("'")[1])
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
            seen = True
        
        if page == 20 or seen:
            form_data = {
                "typebien": response.meta.get('type'),
                "nbpieces": "Indifferent",
                "prixmax": "",
                "zone": "",
                "debut": str(page)
            }
            try:
                yield FormRequest(
                    self.start_urls[0],
                    dont_filter=True,
                    formdata=form_data,
                    callback=self.parse,
                    meta={
                        "page": page+20,
                        "property_type": response.meta.get('property_type')
                    }
                )
            except: pass
        elif self.current_index < len(self.other_type):
            formdata = {
                "typebien": self.other_type,
                "nbpieces": "Indifferent",
                "prixmax": "",
                "zone": "",
                "image.x": "239",
                "image.y": "10",
            }
            yield FormRequest(
                url=self.start_urls[0],
                callback=self.parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":"house",
                    "type": self.other_type,
                }
            )
            self.current_index += 1

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Berriat_Immo_PySpider_france")
        
        title = ''.join(response.xpath("//table[@background='images/fondan.gif']//tr[1]/td[1]//text()").extract())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        
        address = response.xpath("//td[@valign='top' and @width='50%']/up/b/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.strip().split(" ")[0]
            item_loader.add_value("city", address.split(zipcode)[1].strip())
            item_loader.add_value("zipcode", zipcode)
        else:
            address = response.xpath("//td[@valign='top' and @width='50%']/b[2]/text()").get()
            item_loader.add_value("address", address.strip())
            zipcode = address.strip().split(" ")[0]
            item_loader.add_value("city", address.split(zipcode)[1].strip())
            item_loader.add_value("zipcode", zipcode)
            
            
        rent = response.xpath("//td[@valign='top' and @width='50%']/strong//text()[contains(.,'Loyer')]").get()
        if rent:
            rent = rent.split(":")[1].strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        utilities = response.xpath("//td[@valign='top' and @width='50%']/strong//text()[contains(.,'Charges')]").get()
        if utilities:
            utilities = utilities.split(":")[1].strip().split(" ")[0]
            item_loader.add_value("utilities", utilities)

        square_meters = response.xpath("//td[@valign='top' and @width='50%']//text()[contains(.,'m²')]").get()
        if square_meters:
            square_meters = square_meters.split("m²")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters", square_meters)
        
        deposit = response.xpath("//strong//b//text()[contains(.,'Caution')]").get()
        if deposit:
            deposit = deposit.split(":")[1].strip().replace("€", "").strip()
            item_loader.add_value("deposit", deposit)
        
        description = "".join(response.xpath("//p//b//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description.strip())
        
        if "chambre" in description.lower():
            room = description.lower().split("chambre")[0].strip().split(" ")[-1]
            if room.isdigit(): item_loader.add_value("room_count", room)
            elif "une" in room: item_loader.add_value("room_count", "1")
        
        if "studio" in description.lower():
            item_loader.add_value("room_count", "1")
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
            
        
        external_id = response.xpath("substring-after(//span/b/text(),'#')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        energy_label = "".join(response.xpath("//table[contains(@background,'dpe/dpe')]//td/img[contains(@src,'dpe')]/parent::td/following-sibling::td/span/text()").getall())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        images = [x for x in response.xpath("//img/@src[contains(.,'photo')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Berriat Immobilier")
        item_loader.add_value("landlord_phone", "04 76 88 01 02")

        yield item_loader.load_item()