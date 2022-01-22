# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim

class MySpider(Spider):
    name = 'arvern_kwimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source='Arvernkwimmo_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {"url": "https://arvern.kwfrance.com/marketCenter/result/index?view_type=map_list&btn_votre_projet_text=Location+-+Maison&PropertieSearch%5Btransaction_ides%5D%5B%5D=0&PropertieSearch%5Btransaction_ides%5D%5B%5D=0&PropertieSearch%5Btransaction_ides%5D%5B%5D=2&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=2&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprogramme_neuf%5D=0&PropertieSearch%5Bprestige%5D=0&PropertieSearch%5Bbail_type%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bautres%5D=0&btn_budget_text=Budget&PropertieSearch%5Bprix_min%5D=&PropertieSearch%5Bprix_max%5D=&btn_piece_text=Pi%C3%A8ces&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&btn_surface_text=Surface&PropertieSearch%5Bsurface_global_min%5D=&PropertieSearch%5Bsurface_global_max%5D=&PropertieSearch%5Bsurface_terrain_min%5D=&PropertieSearch%5Bsurface_terrain_max%5D=&PropertieSearch%5Bsort%5D=date_desc&PropertieSearch%5Bancien%5D=0&PropertieSearch%5Bneuf%5D=0&PropertieSearch%5Bvia_viager%5D=0&PropertieSearch%5Bprog_neuf%5D=0&PropertieSearch%5Brez_chaussee%5D=0&PropertieSearch%5Brez_jardin%5D=0&PropertieSearch%5Bdernier_etage%5D=0&PropertieSearch%5Bbordmer%5D=0&PropertieSearch%5Bpiscine%5D=0&PropertieSearch%5Bmeuble%5D=0&PropertieSearch%5Bnbr_balcon%5D=0&PropertieSearch%5Bjardin%5D=0&PropertieSearch%5Btennis%5D=0&PropertieSearch%5Bcalme%5D=0&PropertieSearch%5Bsoussol%5D=0&PropertieSearch%5Bnbr_terrrasse%5D=0&PropertieSearch%5Bgardien%5D=0&PropertieSearch%5Bascenseur%5D=0&PropertieSearch%5Bgrenier%5D=0&PropertieSearch%5Betage%5D=0&PropertieSearch%5Bvuemer%5D=0&PropertieSearch%5Bcheminee%5D=0&PropertieSearch%5Bnbr_cave%5D=0&PropertieSearch%5Bnbr_garage%5D=0&PropertieSearch%5Bacces_handicapes%5D=0&PropertieSearch%5Balarme%5D=0&PropertieSearch%5Bdigicode%5D=0&PropertieSearch%5Badsl_fibreoptique%5D=0&PropertieSearch%5Bnbr_wc%5D=0&PropertieSearch%5Bnbr_sdb%5D=0&PropertieSearch%5Bsejour_double%5D=0&PropertieSearch%5Bslc_cuisine%5D=0&PropertieSearch%5Bslc_typechauffage_collectif%5D=0&PropertieSearch%5Bslc_typechauffage_individuel%5D=0&PropertieSearch%5Bslc_typechauffage_mixte%5D=0&PropertieSearch%5Bmode_chauffage_gaz%5D=0&PropertieSearch%5Bmode_chauffage_electrique%5D=0&PropertieSearch%5Bmode_chauffage_fuel%5D=0&PropertieSearch%5Bmode_chauffage_autre%5D=0&PropertieSearch%5Bmode_chauffage_sol%5D=0&PropertieSearch%5Bmeca_chauffage_radiateur%5D=0&PropertieSearch%5Bmeca_chauffage_convecteurs%5D=0&PropertieSearch%5Bexposition_sejour_nord%5D=0&PropertieSearch%5Bexposition_sejour_sud%5D=0&PropertieSearch%5Bexposition_sejour_est%5D=0&PropertieSearch%5Bexposition_sejour_ouest%5D=0&PropertieSearch%5Bprop_url_visite_virtuelle%5D=0&PropertieSearch%5BLienVideo%5D=0&PropertieSearch%5Btypemandat_id%5D=0", "property_type": "house"},
            {"url": "https://arvern.kwfrance.com/marketCenter/result/index?view_type=map_list&btn_votre_projet_text=kiralama+-+Autres+%28%2B1%29&PropertieSearch%5Btransaction_ides%5D%5B%5D=0&PropertieSearch%5Btransaction_ides%5D%5B%5D=0&PropertieSearch%5Btransaction_ides%5D%5B%5D=2&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=2&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprogramme_neuf%5D=0&PropertieSearch%5Bprestige%5D=0&PropertieSearch%5Bbail_type%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bprop_typeides%5D%5B%5D=0&PropertieSearch%5Bautres%5D=0&btn_budget_text=Budget&PropertieSearch%5Bprix_min%5D=&PropertieSearch%5Bprix_max%5D=&btn_piece_text=Pi%C3%A8ces&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_piece%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&PropertieSearch%5Bnbr_chambre%5D%5B%5D=0&btn_surface_text=Surface&PropertieSearch%5Bsurface_global_min%5D=&PropertieSearch%5Bsurface_global_max%5D=&PropertieSearch%5Bsurface_terrain_min%5D=&PropertieSearch%5Bsurface_terrain_max%5D=&PropertieSearch%5Bsort%5D=date_desc&PropertieSearch%5Bancien%5D=0&PropertieSearch%5Bneuf%5D=0&PropertieSearch%5Bvia_viager%5D=0&PropertieSearch%5Bprog_neuf%5D=0&PropertieSearch%5Brez_chaussee%5D=0&PropertieSearch%5Brez_jardin%5D=0&PropertieSearch%5Bdernier_etage%5D=0&PropertieSearch%5Bbordmer%5D=0&PropertieSearch%5Bpiscine%5D=0&PropertieSearch%5Bmeuble%5D=0&PropertieSearch%5Bnbr_balcon%5D=0&PropertieSearch%5Bjardin%5D=0&PropertieSearch%5Btennis%5D=0&PropertieSearch%5Bcalme%5D=0&PropertieSearch%5Bsoussol%5D=0&PropertieSearch%5Bnbr_terrrasse%5D=0&PropertieSearch%5Bgardien%5D=0&PropertieSearch%5Bascenseur%5D=0&PropertieSearch%5Bgrenier%5D=0&PropertieSearch%5Betage%5D=0&PropertieSearch%5Bvuemer%5D=0&PropertieSearch%5Bcheminee%5D=0&PropertieSearch%5Bnbr_cave%5D=0&PropertieSearch%5Bnbr_garage%5D=0&PropertieSearch%5Bacces_handicapes%5D=0&PropertieSearch%5Balarme%5D=0&PropertieSearch%5Bdigicode%5D=0&PropertieSearch%5Badsl_fibreoptique%5D=0&PropertieSearch%5Bnbr_wc%5D=0&PropertieSearch%5Bnbr_sdb%5D=0&PropertieSearch%5Bsejour_double%5D=0&PropertieSearch%5Bslc_cuisine%5D=0&PropertieSearch%5Bslc_typechauffage_collectif%5D=0&PropertieSearch%5Bslc_typechauffage_individuel%5D=0&PropertieSearch%5Bslc_typechauffage_mixte%5D=0&PropertieSearch%5Bmode_chauffage_gaz%5D=0&PropertieSearch%5Bmode_chauffage_electrique%5D=0&PropertieSearch%5Bmode_chauffage_fuel%5D=0&PropertieSearch%5Bmode_chauffage_autre%5D=0&PropertieSearch%5Bmode_chauffage_sol%5D=0&PropertieSearch%5Bmeca_chauffage_radiateur%5D=0&PropertieSearch%5Bmeca_chauffage_convecteurs%5D=0&PropertieSearch%5Bexposition_sejour_nord%5D=0&PropertieSearch%5Bexposition_sejour_sud%5D=0&PropertieSearch%5Bexposition_sejour_est%5D=0&PropertieSearch%5Bexposition_sejour_ouest%5D=0&PropertieSearch%5Bprop_url_visite_virtuelle%5D=0&PropertieSearch%5BLienVideo%5D=0&PropertieSearch%5Btypemandat_id%5D=0", "property_type": "house"},
        ]  # LEVEL 1

        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='img-container']//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})

        # pagination = response.xpath("//a[@class='page_suivante']/@href").get()
        # if pagination:
        #     url = response.urljoin(pagination)
        #     yield Request(url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status=response.xpath("normalize-space((//title//text())[1])").get()
        if "bureaux" not in status.lower():
            item_loader.add_value("external_source", "Arvernkwimmo_PySpider_"+ self.country + "_" + self.locale)
            item_loader.add_xpath("title", "normalize-space((//title//text())[1])")
            title = response.xpath("normalize-space((//title//text())[1])").get()
            if title:
                item_loader.add_value("title", title.split("€")[0])
            item_loader.add_value("property_type", response.meta.get('property_type'))
            item_loader.add_value("external_link", response.url)
            
            city = response.xpath("//div[@class='view-column view-column--title']//text()").get()
            if city:
                item_loader.add_value("city", city.replace("\n","").replace(" ",""))

            rent="".join(response.xpath("(//div[@class='view-column view-column--price']//text())[1]").getall())
            if rent:
                item_loader.add_value("rent", rent.replace(" ",""))
                item_loader.add_value("currency", "EUR")
            
            square_meters=response.xpath("(//div[@class='result-info__item-title']//text())[1]").get()
            if square_meters:
                item_loader.add_value("square_meters", square_meters.strip())
            
            room_count=response.xpath("(//div[@class='result-info__item-title']//text())[4]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(' ')[0])
            
            # address="".join(response.xpath("//ul/li/div[contains(.,'RÉGION')]//parent::li/div[2]//text()").getall())
            # if address:
            #     item_loader.add_value("address", address.strip())
            
            # zipcode=response.xpath("//ul/li/div[contains(.,'CODE')]//parent::li/div[2]//text()").get()
            # if zipcode:
            #     item_loader.add_value("zipcode", zipcode)
            
            external_id=response.xpath("//div[@class='view-about__code']//text()").get()
            if external_id:
                item_loader.add_value("external_id", external_id.split('Ref. ')[1].strip())

            desc="".join(response.xpath("//div[@class='view-about__desc']//text()").getall())
            if desc:
                item_loader.add_value("description", desc.strip().replace("\n",""))
                
            images=[x for x in response.xpath("//div[@class='view-gallery__slider-item']//img//@src").getall()]
            if images:
                item_loader.add_value("images", images)
                item_loader.add_value("external_images_count", str(len(images)))
            
            item_loader.add_value("landlord_name","AGENT KW")
            item_loader.add_value("landlord_phone","04.73.93.97.90")
            
            # floor=response.xpath("//div[@id='accordion']//div[contains(.,'Etage')]/p/text()").get()
            # if floor:
            #     item_loader.add_value("floor", floor.strip())
            
            # elevator=response.xpath("//div[@id='accordion']//div[contains(.,'Ascenseur')]/p/text()[contains(.,'Oui')]").get()
            # if elevator:
            #     item_loader.add_value("elevator",True)
            
            # terrace=response.xpath("//div[@id='accordion']//div[contains(.,'Nombre de terrasses')]/p/text()[not(contains(.,'Non'))]").get()
            # if terrace:
            #     item_loader.add_value("terrace",True)
                
            # swimming_pool=response.xpath("//div[@id='accordion']//div[contains(.,'Piscine')]/p/text()[not(contains(.,'Non'))]").get()
            # if swimming_pool:
            #     item_loader.add_value("swimming_pool",True)
            
            yield item_loader.load_item()