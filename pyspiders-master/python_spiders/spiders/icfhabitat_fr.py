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
    name = 'icfhabitat_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    start_urls = ["https://www.icfhabitat.fr/annonces/logements/localites"]

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//ul[@class='bullets']/li/a/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url,
                callback=self.jump,
            )

    def jump(self, response):
        for item in response.xpath("//div[@class='icf-annonces-seo-list-annonce-body']/a"):
            prop_type = item.xpath("./text()").get()
            if get_p_type_string(prop_type):
                prop_type = get_p_type_string(prop_type)
            else:
                prop_type = None
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': prop_type})
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Icfhabitat_PySpider_france")

        external_id = response.url.split('/')[-1]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//span[@itemprop='address']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split('(')[-1].split(')')[0].strip())
            item_loader.add_value("city", address.split('(')[0].strip())
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//div[@itemprop='description']//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//div[contains(text(),'chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split('chambre')[0].strip().split(' ')[-1].strip())
        
        bathroom_count = response.xpath("//text()[contains(.,'salle de bain')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", '1')

        rent = response.xpath("//div[@id='icf-annonce-resume-loyer-montant']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '')
            item_loader.add_value("rent", int(float(rent.replace(",","."))))
            item_loader.add_value("currency", 'EUR')
        
        images = [response.urljoin(x) for x in response.xpath("//div[@id='icf-annonce-focus-image']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        energy_label = response.xpath("//div[contains(@id,'dpe-e')]/img/@src").get()
        if energy_label:
            energy_label = energy_label.split('/')[-1].split('.')[0].strip()
            if energy_label.upper() in ['A', 'B', 'C', 'D', 'E', 'F', 'G']:
                item_loader.add_value("energy_label", energy_label)

        utilities = response.xpath("//div[contains(@id,'charges')]/text()").get()
        if utilities:
            utilities= utilities.split(':')[-1].split('€')[0].strip().replace(' ', '')
            item_loader.add_value("utilities",int(float(utilities.replace(",","."))))

        item_loader.add_xpath("landlord_name", "//div[@id='icf-annonce-bottom-name']/text()")
        landlord_phone = response.xpath("//div[@id='icf-annonce-bottom-address']//text()[contains(.,'Tél accueil')]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone.split(':')[-1].strip())
        else:
            item_loader.add_value("landlord_phone", "03 21 77 36 36")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "commercial" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("appartement" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "maison" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None