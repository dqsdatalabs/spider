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
    name = 'agenceimmodutaur_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.agenceimmodutaur.com/search.php?a=2&b%5B%5D=appt&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&transact=&neuf=&agence=&view=&ajax=1&facebook=1&start=0&&_=1615880453003", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.agenceimmodutaur.com/search.php?a=2&b%5B%5D=house&c=&radius=0&d=0&e=illimit%C3%A9&f=0&x=illimit%C3%A9&transact=&neuf=&agence=&view=&ajax=1&facebook=1&start=0&&_=1615880453003",
                 "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get("page", 12)
        seen = False

        for item in response.xpath("//a[contains(.,'Détails')]/@href").getall():
            seen = True
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type": response.meta["property_type"]})
        
        if page == 12 or seen:
            follow_url = response.url.replace("&start=" + str(page - 12), "&start=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type": response.meta["property_type"], "page": page + 12})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        prop_type = response.xpath("//tr/td[contains(.,'Type')]/following-sibling::td/text()").get()
        if "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta["property_type"])
            
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agenceimmodutaur_PySpider_france")
        
        title = "".join(response.xpath("//td[@itemprop='name']//text()").getall())
        item_loader.add_value("title", title)
        
        rented_control = response.xpath("//div[@class='band_rotate']").get()
        if rented_control:
            return

        rent = response.xpath("//td[@itemprop='price']/span/text()").get()
        if rent:
            rent = rent.split("€")[0].strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("//tr/td[contains(.,'Référence')]/following-sibling::td/text()").get()
        item_loader.add_value("external_id", external_id)
        
        import dateparser
        available_date = response.xpath("//tr/td[contains(.,'Disponibilité')]/following-sibling::td/text()").get()
        if available_date and "Immédiate" not in available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        street = response.xpath("//tr/td[contains(.,'Secteur')]/following-sibling::td//text()").get()
        address = "".join(response.xpath("//tr/td[contains(.,'Ville')]/following-sibling::td//text()").getall())
        if street or address:
            item_loader.add_value("address", f"{street} {address}".strip())
        
        zipcode = response.xpath("//tr/td[contains(.,'Ville')]/following-sibling::td/span/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
            
        city = response.xpath("//tr/td[contains(.,'Ville')]/following-sibling::td/span[1]//text()").get()
        if city:
            item_loader.add_value("city", city.strip())

        square_meters = response.xpath("//tr/td[contains(.,'Surface')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", int(float(square_meters)))
        
        room_count = response.xpath("//tr/td[contains(.,'Pièces')]/following-sibling::td/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//tr/td[contains(.,'Pièces')]/following-sibling::td/text()").get()
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//tr/td[contains(.,'Salle')]/following-sibling::td/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
            
        floor = response.xpath("//tr/td[contains(.,'Étage')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        furnished = response.xpath("//tr/td[contains(.,'Ameublement')]/following-sibling::td/text()").get()
        if furnished and "non" not in furnished.lower():
            item_loader.add_value("furnished", True)
        
        balcony = response.xpath("//tr/td[contains(.,'Balcon')]/following-sibling::td/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//tr/td[contains(.,'Ascenseur')]/following-sibling::td/text()").get()
        if elevator and "oui" in elevator.lower():
            item_loader.add_value("elevator", True)
        
        swimming_pool = response.xpath("//tr/td[contains(.,'Piscine')]/following-sibling::td/text()").get()
        if swimming_pool and "oui" in swimming_pool.lower():
            item_loader.add_value("swimming_pool", True)
        
        utilities = response.xpath("//tr/td[contains(.,'Charge')]/following-sibling::td/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        parking = response.xpath("//tr/td[contains(.,'Stationnement')]/following-sibling::td/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[contains(@class,'parking')]//text()[contains(.,'Parking')]").get()
            if parking:
                item_loader.add_value("parking", True)
        
        terrace = response.xpath("//tr/td[contains(.,'Terrasse')]/following-sibling::td/text()[.!='0']").get()
        if terrace:
            item_loader.add_value("terrace", True)
            
        energy_label = response.xpath("//b[contains(.,'Consommations énergétiques')]/parent::div/parent::div//b[contains(@class,'dpe-letter-active')]/text()").get()
        if energy_label:
            energy_label = energy_label.split(":")[0].strip()
            item_loader.add_value("energy_label", energy_label)
        
        latitude_longitude = response.xpath("//script[contains(.,'setView([')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('setView([')[1].split(',')[0]
            longitude = latitude_longitude.split('setView([')[1].split(',')[1].split(']')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        description = " ".join(response.xpath("//div[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        images = [x for x in response.xpath("//a[@class='rsImg']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        landlord_name = "".join(response.xpath("//span[@class='firstname']/text() |//span[@class='lastname']/text()").getall())
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone= "".join(response.xpath("//span[@itemprop='telephone']/text()").getall())
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        else:
            landlord_phone= "".join(response.xpath("//span[@itemprop='telephone']//text()").getall())
            if landlord_phone:
                item_loader.add_value("landlord_phone", landlord_phone)
            
        landlord_email= "".join(response.xpath("//span[@itemprop='email']/script/text()").getall())
        if landlord_email:
            landlord_email = landlord_email.split("'")
            item_loader.add_value("landlord_email", f"{landlord_email[1]}@{landlord_email[3]}")
        else:
            item_loader.add_value("landlord_email", "agence@agenceimmodutaur")
        
        deposit = response.xpath("//text()[contains(.,'Dépôt de garantie ')]").get()
        if deposit:
            deposit = deposit.split("Dépôt de garantie")[1].split("€")[0].strip().replace(" ","")
            item_loader.add_value("deposit", deposit)

        if not item_loader.get_collected_values("bathroom_count"):
            bathroom_count = response.xpath('//td[contains(text(),"Salle d\'eau")]/following-sibling::td/text()').get()
            if bathroom_count: item_loader.add_value("bathroom_count", bathroom_count.strip())

        if not item_loader.get_collected_values("landlord_name"): item_loader.add_value("landlord_name", "AGENCE DU TAUR")
        
        yield item_loader.load_item()