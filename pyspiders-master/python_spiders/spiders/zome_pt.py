# -*- coding: utf-8 -*-
# Author: Ahmed Atef
import scrapy
from scrapy import Request, FormRequest
from scrapy.utils.response import open_in_browser
from scrapy.loader import ItemLoader
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme

from ..loaders import ListingLoader
from ..helper import *
from ..user_agents import random_user_agent

import requests
import re
import time
from urllib.parse import urlparse, urlunparse, parse_qs
import json
from w3lib.html import remove_tags


class zomeSpider(scrapy.Spider):

    country = 'portgual'
    locale = 'pt'
    execution_type = 'testing'
    name = f'zome_{locale}'
    external_source = f"zome_PySpider_{country}_{locale}"
    allowed_domains = ['zome.com']
    
    post_url = "https://www.zome.pt/en/modules/PESQUISA_DE_IMOVEIS/searchByForm"
    csrf_token = None
    
    position = 1
    
    headers={
             "Content-Type": "text/html; charset=UTF-8",
             "Accept": "*/*", 
             "Accept-Encoding": "gzip, deflate, br",
            }

    def start_requests(self):
        
        responseToken = requests.get(f"https://www.zome.pt/en/modules/PESQUISA_DE_IMOVEIS/getToken")
        responseTokenData = responseToken.json()
        self.csrf_token = responseTokenData['token']
        
        start_urls = [
            {
                'formData': {
                    "csrf_token":self.csrf_token,
                    "actionRequest":"getUrlFriendly",
                    "json":"1",
                    "params":"localizacaoImovel=&negocio=2&tipo=1&tipoImovel=MQ%3D%3D&condImovel=&quartosImovel=&fltPrecoDeImovel=&fltPrecoAteImovel=&fltAreaMin=&fltConsultor=&fltHUB=&fltHUBZona=&order=",
                    "pageSize": "100",
                    "page": "1",
                },
                'property_type': 'apartment',
            },
            {
                "formData":{
                    "csrf_token":self.csrf_token,
                    "actionRequest":"getUrlFriendly",
                    "json":"1",
                    "params":"localizacaoImovel=&negocio=2&tipo=1&tipoImovel=Mg%3D%3D&condImovel=&quartosImovel=&fltPrecoDeImovel=&fltPrecoAteImovel=&fltAreaMin=&fltConsultor=&fltHUB=&fltHUBZona=&order=",
                    "pageSize": "100",
                    "page": "1",
                },
                'property_type': 'house',
            },
        ]

        for url in start_urls:
            yield FormRequest(url=self.post_url, callback=self.parse, formdata=url.get('formData'), dont_filter=True, meta=url)


    def parse(self, response):
    
        res = response.json()['data'][0]['listings'][0]
        html = Selector(text=res)
        
        cards = html.css(".listagem.tab .dib.modulo")

        for index, card in enumerate(cards):

            if card.css(".mod_fim_valor_vendido::text, .mod_fim_valor.notranslate::text").get() in ["Rented", "All Inclusive", "Reserved", "Reservado", "Price on request", "Preço sob consulta"]:
                continue

            position = self.position

            card_url = card.css("::attr(data-urlview)").get()

            phoneid = card.css("::attr(data-id)").get()
            
            dataUsage = {
                "position": position,
                "card_url": card_url,
                "phoneid": phoneid,
                **response.meta
            }

            zomeSpider.position += 1
            yield Request(card_url, callback=self.parseApartment, dont_filter=True, meta=dataUsage)

        if len(cards) > 0:

            nextPageUrlNumber = int(response.meta['formData']['page'].encode("ascii")) + 1
            response.meta['formData']['page'] = str(nextPageUrlNumber)
            if nextPageUrlNumber:
                yield FormRequest(  url = self.post_url, 
                                    callback = self.parse, 
                                    formdata=response.meta['formData'], 
                                    dont_filter=True,
                                    meta = response.meta)

    def parseApartment(self, response):
        
        rent = response.css(".z_preco.notranslate::text").get()
        
        if rent:
            rent = remove_white_spaces(rent).replace(" ", "")
            rent = extract_number_only(rent).replace(".", "")
            rent = str(rent).split(".")[0]
        else:
            rent = None
            return

        currency = "EUR"

        property_type = response.meta['property_type']

        position = response.meta['position']


        external_id = response.css(".s_id strong::text").get()
        if external_id:
            external_id = remove_white_spaces(external_id)



        square_meters = response.css(".topicos_hor li:contains('GROSS AREA: ') strong::text").getall()
        if square_meters:
            square_meters = " ".join(square_meters)
            square_meters = remove_white_spaces(square_meters).split(",")[0]
            square_meters = extract_number_only(square_meters).replace(".", "")


        room_count = response.css("li.dib.vam:contains('Room') ::text").getall()
        if room_count:
            room_count = " ".join(room_count)
            room_count = remove_white_spaces(room_count)
            room_count = extract_number_only(room_count).replace(".", "")


        bathroom_count = response.css("li.dib.vam:contains('WC') ::text").getall()
        if bathroom_count:
            bathroom_count = " ".join(bathroom_count)
            bathroom_count = remove_white_spaces(bathroom_count)
            bathroom_count = extract_number_only(bathroom_count).replace(".", "")


        description_id = response.css(".s_desc .z_vermais_bt::attr('data-id')").get()
        if description_id:
            
            formData= {
                "csrf_token":self.csrf_token,
                "id":description_id,
            }
            
            responseDesc = requests.post(f"https://www.zome.pt/en/modules/PESQUISA_DE_IMOVEIS/getDescriptionListing", data=formData)
            responseDescData = responseDesc.json()
            description = responseDescData['txt']

        title = response.css(".z_i_info_in h2::text").getall()
        if title:
            title = " ".join(title)
            title = remove_white_spaces(title)

        address = response.css("h3.notranslate::text").getall()
        if address:
            address = " ".join(address)
            address = remove_white_spaces(address)

        city = None
        if address:
            city = address.split()[-1]       
        
        zipcode = None
        longitude = None
        latitude = None
        
        coordenadas = response.css("#mapa_embed_streetView::attr('data-coordenadas')").get()
        if coordenadas:
            coordenadas = coordenadas.split("###")
            longitude = coordenadas[1]
            latitude = coordenadas[0]
        
        try:
            if latitude and longitude:
                responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")
                responseGeocodeData = responseGeocode.json()
                zipcode = responseGeocodeData['address']['Postal'] + " " + responseGeocodeData['address']['PostalExt']
        except Exception as err:
            zipcode = None

        images = response.css('.swiper-slide img::attr(data-src)').getall()
        external_images_count = len(images)

        parking = response.css(".swp_in .dib.vam:contains('garage') span::text").get() or response.css(".swp_in .dib.vam:contains('parking') span::text").get()  or response.css(".swp_in .dib.vat span:contains('Parking')::text").get()
        if parking:
            parking = True
        else:
            parking = False
        
        elevator = response.css(".swp_in .dib.vat span:contains('Lift')::text").get() 
        if elevator:
            elevator = True
        else:
            elevator = False

        balcony = response.css(".swp_in .dib.vat span:contains('Balcony')::text").get()
        if balcony:
            balcony = True
        else:
            balcony = False

        terrace = response.css(".swp_in .dib.vat span:contains('Terrace')::text").get()
        if terrace:
            terrace = True
        else:
            terrace = False
        
        swimming_pool = response.css(".swp_in .dib.vat span:contains('Pool')::text").get()
        if swimming_pool:
            swimming_pool = True
        else:
            swimming_pool = False
        
        washing_machine = response.css(".swp_in .dib.vat span:contains('washing machine')::text").get()
        if washing_machine:
            washing_machine = True
        else:
            washing_machine = False


        landlord_name = response.css(".c_consul_nome::text").get()
        if landlord_name:
            landlord_name = remove_white_spaces(landlord_name)

        landlord_email = "info@zome.pt"
            
        formData= {
            "csrf_token":self.csrf_token,
            "phoneid":response.meta['phoneid'],
        }
        
        responsePhone = requests.post(f"https://www.zome.pt/en/modules/EQUIPA/getPhoneToObject", data=formData)
        responsePhoneData = responsePhone.json()
        landlord_phone = responsePhoneData['number_href']

        if rent:
            item_loader = ListingLoader(response=response)

            item_loader.add_value("external_link", response.url)
            item_loader.add_value("external_source", self.external_source)
            item_loader.add_value("external_id", external_id)
            item_loader.add_value("title", title)
            item_loader.add_value("description", description)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("address", address)
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("property_type", property_type)
            item_loader.add_value("square_meters", square_meters)
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", external_images_count)
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", currency)
            item_loader.add_value("parking", parking)
            item_loader.add_value("elevator", elevator)
            item_loader.add_value("balcony", balcony)
            item_loader.add_value("terrace", terrace)
            item_loader.add_value("swimming_pool",swimming_pool)
            item_loader.add_value("washing_machine", washing_machine)
            item_loader.add_value("landlord_name", landlord_name)
            item_loader.add_value("landlord_email", landlord_email)
            item_loader.add_value("landlord_phone", landlord_phone)
            item_loader.add_value("position", position)

            yield item_loader.load_item()
