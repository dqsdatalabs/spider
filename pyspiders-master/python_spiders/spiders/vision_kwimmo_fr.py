# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math

class MySpider(Spider):
    name = 'vision_kwimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Visionkwimmo_PySpider_france_fr"

    def start_requests(self):
        start_urls = [
            {"url": "https://vision.kwfrance.com/marketCenter/result/index?view_type=map_list&btn_votre_projet_text=Location+-+Appartement&PropertieSearch%5Btransaction_ides%5D%5B%5D=0&PropertieSearch%5Btransaction_ides%5D%5B%5D=0&PropertieSearch%5Btransaction_ides%5D%5B%5D=2&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=1&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprogramme_neuf%5D=0&PropertieSearch%5Bprestige%5D=0&PropertieSearch%5Bbail_type%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bautres%5D=0&btn_budget_text=Budget&PropertieSearch%5Bprix_min%5D=&PropertieSearch%5Bprix_max%5D=&btn_piece_text=Pi%C3%A8ces&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&btn_surface_text=Surface&PropertieSearch%5Bsurface_global_min%5D=&PropertieSearch%5Bsurface_global_max%5D=&PropertieSearch%5Bsurface_terrain_min%5D=&PropertieSearch%5Bsurface_terrain_max%5D=&PropertieSearch%5Bsort%5D=date_desc&PropertieSearch%5Bancien%5D=0&PropertieSearch%5Bneuf%5D=0&PropertieSearch%5Bvia_viager%5D=0&PropertieSearch%5Bprog_neuf%5D=0&PropertieSearch%5Brez_chaussee%5D=0&PropertieSearch%5Brez_jardin%5D=0&PropertieSearch%5Bdernier_etage%5D=0&PropertieSearch%5Bbordmer%5D=0&PropertieSearch%5Bpiscine%5D=0&PropertieSearch%5Bmeuble%5D=0&PropertieSearch%5Bnbr_balcon%5D=0&PropertieSearch%5Bjardin%5D=0&PropertieSearch%5Btennis%5D=0&PropertieSearch%5Bcalme%5D=0&PropertieSearch%5Bsoussol%5D=0&PropertieSearch%5Bnbr_terrrasse%5D=0&PropertieSearch%5Bgardien%5D=0&PropertieSearch%5Bascenseur%5D=0&PropertieSearch%5Bgrenier%5D=0&PropertieSearch%5Betage%5D=0&PropertieSearch%5Bvuemer%5D=0&PropertieSearch%5Bcheminee%5D=0&PropertieSearch%5Bnbr_cave%5D=0&PropertieSearch%5Bnbr_garage%5D=0&PropertieSearch%5Bacces_handicapes%5D=0&PropertieSearch%5Balarme%5D=0&PropertieSearch%5Bdigicode%5D=0&PropertieSearch%5Badsl_fibreoptique%5D=0&PropertieSearch%5Bnbr_wc%5D=0&PropertieSearch%5Bnbr_sdb%5D=0&PropertieSearch%5Bsejour_double%5D=0&PropertieSearch%5Bslc_cuisine%5D=0&PropertieSearch%5Bslc_typechauffage_collectif%5D=0&PropertieSearch%5Bslc_typechauffage_individuel%5D=0&PropertieSearch%5Bslc_typechauffage_mixte%5D=0&PropertieSearch%5Bmode_chauffage_gaz%5D=0&PropertieSearch%5Bmode_chauffage_electrique%5D=0&PropertieSearch%5Bmode_chauffage_fuel%5D=0&PropertieSearch%5Bmode_chauffage_autre%5D=0&PropertieSearch%5Bmode_chauffage_sol%5D=0&PropertieSearch%5Bmeca_chauffage_radiateur%5D=0&PropertieSearch%5Bmeca_chauffage_convecteurs%5D=0&PropertieSearch%5Bexposition_sejour_nord%5D=0&PropertieSearch%5Bexposition_sejour_sud%5D=0&PropertieSearch%5Bexposition_sejour_est%5D=0&PropertieSearch%5Bexposition_sejour_ouest%5D=0&PropertieSearch%5Bprop_url_visite_virtuelle%5D=0&PropertieSearch%5BLienVideo%5D=0&PropertieSearch%5Btypemandat_id%5D=0", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                             callback=self.parse,
                             meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[@class='result-item']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Visionkwimmo_PySpider_"+ self.country + "_" + self.locale)

        title = response.xpath("//title/text()").extract_first()
        if title:
            item_loader.add_value("title", title.strip())

        item_loader.add_value("external_link", response.url)
        external_id = response.xpath("//div[@class='view-about__code']//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(".")[1].strip())
   
        description = response.xpath("//div[@class='view-about__desc']//text()").get()
        if description:
            item_loader.add_value("description", description.strip())

        city = response.xpath("//div[contains(@class,'view-column--title')]//text()").get()
        address = response.xpath("//div[contains(@class,'view-column--title')]//text()").get()

        item_loader.add_value("address", address.strip())
        item_loader.add_value("city", city.strip())
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        square_meters = "".join(response.xpath("//section//li[div[contains(@class,'square')]]/div[2]/text()").getall())
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            square_meters = math.ceil(float(square_meters))
        item_loader.add_value("square_meters", str(square_meters))
        
        room_count = response.xpath("//li[contains(@class,'result-info__item')]//div[contains(@class,'result-info__item-title')]//text()[contains(.,'pièces')]").get()
        if room_count:
            room_count = room_count.split("pièces")
            item_loader.add_value("room_count", room_count)
        
        images = [response.urljoin(x) for x in response.xpath("//img[contains(@class,'slider-item__img')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        price = "".join(response.xpath("//div[contains(@class,'view-column--price')]/text()").getall())
        if price:
            price = price.strip()

        item_loader.add_value("rent", price)
        item_loader.add_value("currency", "EUR")
        
        elevator = response.xpath("//li[div[contains(.,'Ascenseur')]]/div[2]/text()").get()
        if elevator:
            if elevator.lower() == "oui":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        energy_label= "".join(response.xpath("//li[contains(@class,'active view-diagnostic__condition-item view-diagnostic__condition-item--yellow')]//text()").get())
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip().split(" ")[0])
        
        parking = response.xpath("//li[div[contains(.,'Stationnement')]]/div[2]/text()[contains(.,'Garage')]").get()
        if parking:
            if parking.lower() != "non":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        
        latitude = response.xpath("//section[@id='propertie-view']//@data-lat").get()
        if latitude:
             item_loader.add_value("latitude", latitude)
        longitude =response.xpath("//section[@id='propertie-view']//@data-long").get()
        if longitude:
             item_loader.add_value("longitude", longitude)

        landlord_name = response.xpath("//a[contains(@class,'share-info__row--name')]/text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//a[contains(@class,'share-info__tel')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()