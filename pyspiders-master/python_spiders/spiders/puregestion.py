# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import json
import html
from scrapy import FormRequest
from ..loaders import ListingLoader
class PuregestionSpider(scrapy.Spider):
    name = "puregestion"
    allowed_domains = ["www.pure-gestion.com"]
    start_urls = ["https://www.pure-gestion.com/trouver-location/?type_bien=appartement"]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    custom_settings = {
        "PROXY_ON":"True"
    }
    thousand_separator=','
    scale_separator='.'
    headers = {
        'accept': '*/*',
        'sec-fetch-dest': 'empty',
        'x-requested-with': 'XMLHttpRequest',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }
    def parse(self, response):
        formdata = {
            "action": "more_annonces",
            "where[type_bien][]": "appartement",
            "offset": "0",
            "entite": "location",
            "widget_id": "",
        }

        yield FormRequest(
                url="https://www.pure-gestion.com/wp-admin/admin-ajax.php",
                formdata=formdata,
                callback=self.jump,
                headers = self.headers, 
                meta={'property_type': "apartment", "formdata":formdata, "handle_httpstatus_list": [307]},
                dont_filter=True,
            )

        
        formdata2 = {
            "action": "more_annonces",
            "where[type_bien][]": "maison",
            "offset": "0",
            "entite": "location",
            "widget_id": "",
        }

        yield FormRequest(
                url="https://www.pure-gestion.com/wp-admin/admin-ajax.php",
                formdata=formdata2,
                callback=self.jump,
                headers = self.headers, 
                meta={'property_type': "house", "formdata":formdata2, "handle_httpstatus_list": [307]},
                dont_filter=True,
            )

    def jump(self, response, **kwargs):
        if response.status == 307:
            yield FormRequest(
                url="https://www.pure-gestion.com/wp-admin/admin-ajax.php",
                formdata=response.meta["formdata"],
                callback=self.jump,
                headers = self.headers, 
                meta={'property_type': response.meta["property_type"], "formdata":response.meta["formdata"], "handle_httpstatus_list": [307]},
                dont_filter=True,
            )
            return
        else:
            datas = json.loads(response.text)
            data_json = datas['data']['annonces']
            for data in data_json:
                external_id = str(data['reference'])
                rent = str(data['prix_max']) + "€"
                square_meters = str(data['surface_max'])
                room_count = str(data['nbre_pieces'])
                description = html.unescape(data['description'])
                city = data['ville']
                zipcode = data['code_postal']
                address = zipcode + ' ' + city 
                longitude = str(data['longitude'])
                latitude = str(data['latitude'])
                images = []
                for img in data['images']:
                    images.append(img['image'])
                if 'apartment' in response.meta.get('property_type'):
                    property_type = response.meta.get('property_type')
                    external_link = "https://www.pure-gestion.com/location-appartement-{}-{}/{}/".format(zipcode, city.replace(" ",""), external_id)
                else:
                    property_type = response.meta.get('property_type')
                    external_link = "https://www.pure-gestion.com/location-appartement-{}-{}/{}/".format(zipcode, city.replace(" ",""), external_id)
                item_loader = ListingLoader(response=response)
                if property_type:
                    item_loader.add_value('property_type', property_type)
                item_loader.add_value('external_id', external_id)
                item_loader.add_value('external_link', external_link)
                item_loader.add_value('title', address)
                item_loader.add_value('address', address)
                item_loader.add_value('city', city)
                item_loader.add_value('zipcode', zipcode)
                item_loader.add_value('description', description)
                item_loader.add_value('rent_string', rent)
                utilities = data['charges_mensuelles']
                if utilities:
                    item_loader.add_value("utilities", utilities)
                if longitude:
                    item_loader.add_value('longitude', longitude)
                if latitude:
                    item_loader.add_value('latitude', latitude)
                item_loader.add_value('images', images)
                item_loader.add_value('square_meters', square_meters)
                item_loader.add_value('room_count', room_count)
                item_loader.add_value('landlord_name', '.Pure Gestion')
                item_loader.add_value('landlord_email', 'location@pure-gestion.com')
                item_loader.add_value('landlord_phone', '04 78 17 14 17')
                item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
                yield item_loader.load_item()
