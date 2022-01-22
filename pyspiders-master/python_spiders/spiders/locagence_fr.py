# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import re
import js2xml
import lxml.etree
import scrapy
from scrapy import Selector
from ..helper import extract_number_only, extract_rent_currency, remove_white_spaces,format_date
from ..loaders import ListingLoader
from math import ceil

class LocagenceFrSpider(scrapy.Spider):
    name = 'locagence_fr'
    allowed_domains = ['www.locagence.fr']
    start_urls = ['https://www.locagence.fr/a-louer/1']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    thousand_separator=' '
    scale_separator=','
    position = 0

    def start_requests(self):     
        start_urls = ["https://www.locagence.fr/a-louer/1"]   
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                )

    def parse(self, response, **kwargs):
        listings = response.xpath('//div[contains(@class," col-md-6 ")]')
        
        for property_item in listings:
            title = property_item.xpath('.//h1/a/text()').extract_first()
            if 'parking' in title.lower() or 'box' in title.lower() or 'garage' in title.lower():
                continue
            external_link = f"https://www.locagence.fr{property_item.xpath('.//h1/a/@href').extract_first()}"

            yield scrapy.Request(
                url = external_link,
                callback=self.get_property_details,
                meta={
                    'external_link' : external_link,
                    'title' : title
                    })

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('external_link'))
        external_id = response.xpath('.//span[contains(@itemprop,"productID")]/text()').extract_first().split(' ')[1]
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('title', response.meta.get('title'))
        images = response.xpath('.//li[contains(@data-thumb,"jpg")]/img/@src').extract()
        images = [f'https:{img}' for img in images]
        item_loader.add_value('images', images)
        item_loader.add_xpath('description', './/p[@itemprop="description"]/text()')

        rent = "".join(response.xpath('//div[@class="value-prix"]/text()').getall())
        if rent:
            rent = rent.replace("€","").strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        deposit = response.xpath('.//span[contains(text(),"garantie")]/../span[2]/text()').extract_first()
        if deposit:
            deposit = deposit.replace("€","").strip().replace(" ","")
            item_loader.add_value('deposit', deposit)

        zipcode = response.xpath('.//span[contains(text(),"Code postal")]/../span[2]/text()').extract_first()
        if zipcode:
            item_loader.add_value('zipcode', remove_white_spaces(zipcode))        
        city = response.xpath('.//span[contains(text(),"Ville")]/../span[2]/text()').extract_first()
        if city:
            item_loader.add_value('city', remove_white_spaces(city))
        square_meters = response.xpath('.//span[contains(text(),"m²")]/../span[2]/text()').extract_first()
        if square_meters:
            square_meters = str(int(ceil(float(extract_number_only(remove_white_spaces(square_meters),thousand_separator=' ',scale_separator=',')))))
            item_loader.add_value('square_meters', square_meters)
        room_count = response.xpath('.//span[contains(text(),"chambre")]/../span[2]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', remove_white_spaces(room_count))
        elif not room_count:
            room_count = response.xpath('.//span[contains(text(),"pièces")]/../span[2]/text()').extract_first()
            if room_count:
                item_loader.add_value('room_count', remove_white_spaces(room_count))
        floor = response.xpath('.//span[contains(text(),"Etage")]/../span[2]/text()').extract_first()
        if floor:
            item_loader.add_value('floor', floor)
        # https://www.locagence.fr/1432-appartement-mandelieu-la-napoule-1-piece-s.html
        elevator = response.xpath('.//span[contains(text(),"Ascenseur")]/../span[2]/text()').extract_first()
        if elevator:
            if remove_white_spaces(elevator) == 'OUI':
                item_loader.add_value('elevator', True)
            elif remove_white_spaces(elevator) == 'NON':
                item_loader.add_value('elevator', False)
        furnished = response.xpath('.//span[contains(text(),"Meublé")]/../span[2]/text()').extract_first()
        if furnished:
            if remove_white_spaces(furnished) == 'OUI':
                item_loader.add_value('furnished', True)
            elif remove_white_spaces(furnished) == 'NON':
                item_loader.add_value('furnished', False)
        
        bathroom_count = response.xpath('.//span[contains(text(),"salle")]/../span[2]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', remove_white_spaces(bathroom_count))
            
        utilities = response.xpath("//span[contains(text(),'Charge')]/../span[2]/text()").get()
        if utilities:
            utilities = utilities.split("€")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        # https://www.locagence.fr/1453-appartement-mandelieu-la-napoule-2-piece-s-47-m2.html
        terrace = response.xpath('.//span[contains(text(),"Terrasse")]/../span[2]/text()').extract_first()
        if terrace:
            if remove_white_spaces(terrace) == 'OUI':
                item_loader.add_value('terrace', True)
            elif remove_white_spaces(terrace) == 'NON':
                item_loader.add_value('terrace', False)
        balcony = response.xpath('.//span[contains(text(),"Balcon")]/../span[2]/text()').extract_first()
        # https://www.locagence.fr/1432-appartement-mandelieu-la-napoule-1-piece-s.html
        if balcony:
            if remove_white_spaces(balcony) == 'OUI':
                item_loader.add_value('balcony', True)
            elif remove_white_spaces(balcony) == 'NON':
                item_loader.add_value('balcony', False)
        # https://www.locagence.fr/1453-appartement-mandelieu-la-napoule-2-piece-s-47-m2.html
        parking = response.xpath('.//span[contains(text(),"parking")]/../span[2]/text()').extract_first()
        if parking:
            if int(remove_white_spaces(parking))>0:
                item_loader.add_value('parking', True)

        item_loader.add_value('address', item_loader.get_output_value('city')+ ', ' +item_loader.get_output_value('zipcode'))
        javascript = response.xpath('(.//*[contains(text(),"lng")])/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/number/@value').get()
            longitude = selector.xpath('.//property[@name="lng"]/number/@value').get()
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)

        months = {'janvier' : 'January',
                'février' : 'February',
                'mars' : 'March',
                'avril' : 'April',
                'mai' : 'May',
                'juin' : 'June',
                'juillet' : 'July',
                'aout' : 'August',
                'septembre' : 'September',
                'octobre' : 'October',
                'novembre' : 'November',
                'décembre' : 'December'}

        #EG: https://www.locagence.fr/1433-appartement-1-piece-s-27-45-m2.html - 11/01/2020
        availability = re.findall(r'Disponible le (.+)\.',item_loader.get_output_value('description'))
        if availability:
            if re.search(r'\d{2}/\d{2}/\d{4}',availability[0]):
                item_loader.add_value('available_date', format_date(availability[0], date_format='%d/%m/%Y'))
            #Eg: https://www.locagence.fr/1439-appartement-pegomas-2-piece-s-46-m2.html - 10 janvier 2021
            elif re.search(r'(\d{2}) (\w+) (\d{4})',availability[0]):
                availability = re.findall(r'(\d{2}) (\w+) (\d{4})',availability[0])
                date, month, year = availability[0]
                item_loader.add_value('available_date', format_date(date+months[month]+year, date_format='%d%B%Y'))
        #Eg: https://www.locagence.fr/1452-appartement-4-piece-s-117-38-m2.html - 6 janvier 2021
        elif re.findall(r'Disponible à compter du (.+)\.', item_loader.get_output_value('description')):
            availability = re.findall(r'(\d+) (\w+) (\d{4})', item_loader.get_output_value('description'))
            date,month,year = availability[0]
            if len(date) == 1:
                date = '0' + date
                item_loader.add_value('available_date', format_date(date+months[month]+year, date_format='%d%B%Y'))

        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['mobile home','park home','character property',
                'chalet', 'bungalow', 'maison', 'house', 'home', ' villa ',
                'holiday complex', 'cottage', 'semi-detached']
        studio_types = ["studio"]
        if any (i in  item_loader.get_output_value('title').lower() or i in item_loader.get_output_value('description').lower()  for i in studio_types):
            item_loader.add_value('property_type','studio')
        elif any (i in item_loader.get_output_value('title').lower() for i in apartment_types):
            item_loader.add_value('property_type','apartment')
        elif any (i in item_loader.get_output_value('title').lower() for i in house_types):
            item_loader.add_value('property_type','house')
        else:
            prop_type = response.xpath("//h2//text()").get()
            if get_p_type_string(prop_type):
                item_loader.add_value("property_type", get_p_type_string(prop_type))
            else: return
            
        item_loader.add_value('landlord_name','Locagence')
        item_loader.add_value('landlord_phone', '04 93 93 66 66')
        item_loader.add_value('landlord_email', 'locagence@ladresse.com')
        self.position+=1
        item_loader.add_value('position',self.position)
        item_loader.add_value("external_source", "Locagence_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None