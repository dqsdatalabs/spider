# -*- coding: utf-8 -*-
# Author: Noor
import requests
import scrapy
from scrapy import FormRequest

from ..loaders import ListingLoader
import re


class MySpider(scrapy.Spider):
    name = 'cajozzore_it'
    allowed_domains = ['cajozzore.it']
    start_urls = [
        'https://cajozzore.it/ricerca-immobili/case-appartamenti/affitto/']
    execution_type = 'testing'
    country = 'italy'
    locale = 'it'
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    thousand_separator = '.'
    scale_separator = '.'

    def parse(self, response):
        pages_number = int(response.css('.page-link ::text').extract()[-2])
        for i in range(1, pages_number + 1):
            frmdata = {"action": "prop_listing_search",
                       "collect_data": " by_title=&offer-type=affitto&location-by=&min-price=&max-price=&min-area=&max-area=&property-type=case-appartamenti&type-beds=&type-bath=&latt=&long=&distance=&author=",
                       "page_no": str(i)}
            url = 'https://cajozzore.it/ricerca-immobili/case-appartamenti/affitto/'
            yield FormRequest(url, callback=self.parse2, formdata=frmdata, dont_filter=True)

    def parse2(self, response):
        links = response.css('.clr-black').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                dont_filter=True)

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.url)
        address = response.css('.list-style-location li ::text').extract()[3]
        item_loader.add_value('address', address)
        if len(address.split(','))>4:
            item_loader.add_value('city',address.split(',')[-3].strip())
        else:
            city_info=response.css('.list-style-location li ::text').extract()
            if ' Zona :' in city_info:
                city=''
                if '-' in city_info[6]:
                    city = city_info[6].split('-')[-1].replace(')',' ').strip()
                elif '(' in city_info[6]:
                    city = city_info[6].split(' ')[0].replace(')',' ').strip()
                elif ',' in city_info[6]:
                    city = city_info[6].split(',')[0].replace(')',' ').strip()
                else:
                    city = city_info[6]
                item_loader.add_value('city', city)
        d= response.css('.post-desc p::text').extract()
        if d and d[0]:
            description =d[0]
            item_loader.add_value('description', description)
        else:
            description=response.css('p:nth-child(1) .oo9gr5id::text').extract()[0]
            item_loader.add_value('description', description)
        images = response.css('.carousel img').xpath('@src').extract()
        images = [re.sub(r'-\d*x\d*', "", img) for img in images]
        item_loader.add_value('images', images)
        title = response.css('.text-light::text').extract()[0]
        item_loader.add_value('title', title)
        item_loader.add_value('currency', 'EUR')
        item_loader.add_value('landlord_name', 'cajozzoreGold')
        item_loader.add_value('landlord_phone', '091 611 7556')
        item_loader.add_value('landlord_email', 'info@cajozzore.it')
        item_loader.add_value('external_source', self.external_source)
        item_loader.add_value('property_type', 'apartment')
        rent = response.css('.d-flex h3::text').extract()[0][2:]
        item_loader.add_value('rent_string', rent)

        details = response.css('.short-detial ::text').extract()
        stripped_details = [i.strip() for i in details]

        if 'Riferimento:' in stripped_details:
            i = stripped_details.index('Riferimento:')
            value = stripped_details[i+1]
            item_loader.add_value('external_id', value)
        else :
            id=response.xpath('//link').xpath('@href').extract()[-6]
            item_loader.add_value('external_id',id[id.index('=')+1:])

        if 'Locali:' in stripped_details:
            i = stripped_details.index('Locali:')
            value = stripped_details[i+1]
            item_loader.add_value('room_count', int(value))
        if 'Bagni:' in stripped_details:
            i = stripped_details.index('Bagni:')
            value = stripped_details[i+1]
            item_loader.add_value('bathroom_count', int(value))
        if 'Piano:' in stripped_details:
            i = stripped_details.index('Piano:')
            value = stripped_details[i+1]
            item_loader.add_value('floor', value.strip())
        if 'Spese condominiali:' in stripped_details:
            i = stripped_details.index('Spese condominiali:')
            value = stripped_details[i+1][2:]
            item_loader.add_value('utilities', value)
        if 'Superficie:' in stripped_details:
            i = stripped_details.index('Superficie:')
            value = stripped_details[i+1].split(' ')[0]
            item_loader.add_value('square_meters', int(value))

        features=response.css('.clearfix ::text').extract()
        if 'NON arredato' in features:
            item_loader.add_value('furnished',False)
        elif 'Arredato' in features:
            item_loader.add_value('furnished', True)
        if'Balcone' in features:
            item_loader.add_value('balcony', True)
        if 'Posto auto' in features :
            item_loader.add_value('parking', True)
        if 'Terrazza' in features:
            item_loader.add_value('terrace', True)
        if 'Ascensore' in features:
            item_loader.add_value('parking', True)




        loc= response.css('iframe').xpath('@src').extract()
        if loc and loc[0]:
            location_link =loc[0]
            location_regex = re.compile(r'q=([0-9]+\.[0-9]+),([0-9]+\.[0-9]+)')
            ll = location_regex.search(location_link)
            if ll:
                long_lat = str(ll.group())
                lat = long_lat[long_lat.index('=') + 1:long_lat.index(',')]
                long = long_lat[long_lat.index(',') + 1:]
                item_loader.add_value('longitude', long)
                item_loader.add_value('latitude', lat)
                responseGeocode = requests.get(
                    f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={long},{lat}&f=pjson&distance=50000&outSR=")

        else:
            l=response.css('script').extract()[-10]
            lls = [x.group()[1:-1] for x in re.finditer(r'"[0-9]+.[0-9]+"', l)]
            item_loader.add_value('latitude', lls[0])
            item_loader.add_value('longitude', lls[1])
            responseGeocode = requests.get(
                f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={lls[1]},{lls[0]}&f=pjson&distance=50000&outSR=")

        responseGeocodeData = responseGeocode.json()
        zipcode = responseGeocodeData['address']['Postal']
        item_loader.add_value('zipcode',zipcode)

        yield item_loader.load_item()
