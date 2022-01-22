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
    name = 'arobazimmo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.arobazimmo.com/locations"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//h2[@class='ellipsis']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            title = item.xpath("./span/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={"title":title})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        
        title = response.meta["title"]
        prop_type = response.xpath("//span[@class='libelle']/text()").get()
        if "studio" in prop_type.lower():
            item_loader.add_value("property_type", "studio")
        elif get_p_type_string(title):
            item_loader.add_value("property_type", get_p_type_string(title))
        else:
            return

        item_loader.add_value("external_source", "Arobazimmo_PySpider_france")

        external_id = response.url.split('/')[-1].split('-')[0]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address ="".join( response.xpath("//span[contains(.,'Secteur')]/following-sibling::span//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.strip().split('- ')[-1].strip())
            item_loader.add_value("city", address.strip().split('- ')[0].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//h2[contains(.,'Descriptif')]/following-sibling::*/text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//td[contains(.,'Surface')]/following-sibling::td/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split('m')[0].split(',')[0].strip().replace(' ', ''))

        room_count = response.xpath("//td[contains(.,'Chambre')]/following-sibling::td/text()[.!=' 0']").get()
        if not room_count:
            room_count = response.xpath("//td[contains(.,'Pièce')]/following-sibling::td/text()[.!=' 0']").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        
        bathroom_count = response.xpath("//td[contains(.,'SdB')]/following-sibling::td/text()").get()
        if bathroom_count:
            if bathroom_count.split('/')[0] != "0":
                item_loader.add_value("bathroom_count", bathroom_count.split('/')[0].strip())
            else:
                item_loader.add_value("bathroom_count", bathroom_count.split('/')[1].strip())


        rent = response.xpath("//section[contains(text(),'Loyer')]/text()").get()
        if rent:
            rent = "".join(filter(str.isnumeric, rent.split(':')[-1].split('€')[0].split(',')[0].strip()))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        from datetime import datetime
        from datetime import date
        import dateparser
        available_date = response.xpath("//text()[contains(.,'Disponible début')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('but')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//td[contains(.,'Dépot de garantie')]/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split('€')[0].strip().replace(' ', ''))
        
        images = [response.urljoin(x) for x in response.xpath("//div[@class='main-pictures']/div/@data-src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        energy_label = response.xpath("//figure[@class='ec ce']/img/@alt").get()
        if energy_label:
            energy_label = energy_label.split(':')[-1].strip()
            if energy_label in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)

        utilities = response.xpath("//td[contains(.,'Charges')]/following-sibling::td/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split('€')[0].strip().replace(' ', ''))
        
        parking = response.xpath("//td[contains(.,'Parking')]/following-sibling::td/text()").get()
        if parking:
            if int(parking.split('/')[0].strip()) > 0:
                item_loader.add_value("parking", True)
            elif int(parking.split('/')[0].strip()) == 0:
                item_loader.add_value("parking", False)

        floor = response.xpath("//td[contains(.,'Étage')]/following-sibling::td/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        balcony_terrace = response.xpath("//td[contains(.,'Balcon/Terrasse')]/following-sibling::td/text()").get()
        if balcony_terrace:
            balcony = int(balcony_terrace.split('/')[0].strip())
            terrace = int(balcony_terrace.split('/')[-1].strip())
            if balcony > 0:
                item_loader.add_value("balcony", True)
            elif balcony == 0:
                item_loader.add_value("balcony", False)
            if terrace > 0:
                item_loader.add_value("terrace", True)
            elif terrace == 0:
                item_loader.add_value("terrace", False)

        furnished = response.xpath("//td[contains(.,\"Type d'offre\")]/following-sibling::td/text()").get()
        if furnished:
            if 'meubl' in furnished.strip().lower():
                item_loader.add_value("furnished", True)
            elif 'vide' in furnished.strip().lower():
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//td[contains(.,'Ascenseur')]/following-sibling::td/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                item_loader.add_value("elevator", True)
            elif elevator.strip().lower() == 'non':
                item_loader.add_value("elevator", False)

        item_loader.add_xpath("landlord_name", "//td[contains(.,'Dossier suivi par')]/following-sibling::td//text()")
        item_loader.add_xpath("landlord_phone", "//td[contains(.,'Téléphone')]/following-sibling::td//text()")
        item_loader.add_xpath("landlord_email", "//td[contains(.,'E-mail')]/following-sibling::td//text()")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    else:
        return None