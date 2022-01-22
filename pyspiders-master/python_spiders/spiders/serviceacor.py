# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 3:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

def cleanText(text):
    text = ''.join(text.split())
    text = re.sub(r'[^a-zA-Z0-9]', ' ', text).strip()
    return text.replace(" ","_").lower()

def cleanKey(data):
    if isinstance(data,dict):
        dic = {}
        for k,v in data.items():
            dic[cleanText(k)]=cleanKey(v)
        return dic
    else:
        return data

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    if len(zip_city.split(" ")) > 2:
        zipcode, city = (zip_city.split(" ")[0], zip_city.replace(zip_city.split(" ")[0], '')) 
    else:
        zipcode, city = zip_city.split(" ")
    return zipcode, city

class ServiceacorSpider(scrapy.Spider):
    name = 'serviceacor'
    allowed_domains = ['serviceacor']
    start_urls = ['https://www.serviceacor.com/']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator='.'
    scale_separator=','
    
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.serviceacor.com/maisons-a-louer.php?typeFilter=1&priceFilter=',
                'property_type': 'house'},
            {'url': 'https://www.serviceacor.com/maisons-a-louer.php?typeFilter=2&priceFilter=',
                'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})
    
    def parse(self, response, **kwargs):
        pages = response.xpath('//div[contains(@class, "pagination")]/a')
        for page in pages:
            next_link = response.urljoin(page.xpath('./@href').extract_first())
            yield scrapy.Request(
                url=next_link,
                callback=self.parse_first,
                meta={'property_type': response.meta.get('property_type')},
                dont_filter=True
            )

    def parse_first(self, response):
        links = response.xpath('//div[@class="grid"]/div[contains(@class, "item_bien")]/div/a')
        for link in links:
            url = response.urljoin(link.xpath('./@href').extract_first())
            title = ' '.join(link.xpath('./h4[@class="titre"]//text()').extract())
            price_text = link.xpath('./span[@class="price"]/text()').extract_first('')
            if not price_text or 'prix' in price_text.lower():
                continue 
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'property_type': response.meta.get('property_type'), 'title': title, 'price': price_text},
                dont_filter=True
            )
    
    def get_property_details(self, response):
        external_id = response.xpath('//span[contains(text(), "code unique")]/following-sibling::b/text()').extract_first()
        external_link = response.url
        property_type = response.meta.get('property_type')
        title = response.meta.get('title')
        price = response.meta.get('price')
        address = re.search(r'https://maps\.google\.com/maps\?q=(.*?)&', response.text, re.S | re.M | re.I).group(1)
        zipcode, city = extract_city_zipcode(address)
        detail_text = ''.join(response.xpath('//h3[contains(text(), "Description")]/following-sibling::p//text()').extract())
        images = []
        image_links = response.xpath('//div[contains(@class, "slider")]//a')
        for image_link in image_links:
            image_url = response.urljoin(image_link.xpath('./img/@src').extract_first())
            if image_url not in images:
                images.append(image_url)
        square_meters = response.xpath('//img[contains(@src, "maison")]/../text()').extract_first('')
        elevator_text = response.xpath('//span[contains(text(), "Ascenseur")]/following-sibling::b/text()').extract_first('')
        if 'oui' in elevator_text.lower():
            elevator = True
        else:
            elevator = ''
        room_count_text = response.xpath('//img[contains(@src, "lit")]/../text()').extract_first('')
        if room_count_text:
            room_count = re.findall(r'\d+', room_count_text)[0]
        else:
            room_count = ''
        terrace_texts = response.xpath('//span[contains(text(), "Terrasse")]/following-sibling::b/text()').extract_first('')
        if terrace_texts:
            terrace_texts = re.findall(r'\d+', terrace_texts)[0]
            if int(terrace_texts) > 0: 
                terrace = True
        else:
            terrace = ''
        parking_text = response.xpath('//span[contains(text(), "Parking")]/following-sibling::b/text()').extract_first('')
        if 'oui' in parking_text.lower():
            parking = True
        else:
            parking = ''
        if 'lave-vaisselle' in detail_text.lower():
            dishwasher = True
        else:
            dishwasher = ''
        if square_meters and room_count:  
            item_loader = ListingLoader(response=response)
            if response.xpath('//h3[contains(text(), "Intérieur")]/following-sibling::ul/li'):
                temp_dic = {}
                all_tr = response.xpath('//div[contains(@class, "bloc_texte")]')
                for aah_tr in all_tr:
                    for ech_tr in aah_tr.xpath('.//ul/li'):
                        key = ech_tr.xpath('./span//text()').extract_first().strip()
                        vals= ech_tr.xpath('./b//text()').extract_first().strip()
                        temp_dic.update({key:vals})
                temp_dic = cleanKey(temp_dic)
            if 'peb_espec_kwh_m__an' in temp_dic:
                item_loader.add_value('energy_label', temp_dic['peb_espec_kwh_m__an'])
            if 'coordonn_esxy_x' in temp_dic:
                lat = temp_dic['coordonn_esxy_x'].replace(',', '.')
                item_loader.add_value('latitude', str(lat))
            if 'coordonn_esxy_y' in temp_dic:
                lon = temp_dic['coordonn_esxy_y'].replace(',', '.')
                item_loader.add_value('longitude', str(lon))
            if 'chargesmensuelles' in temp_dic:
                item_loader.add_value('utilities', getSqureMtr(temp_dic['chargesmensuelles']))
            item_loader.add_value('external_id', str(external_id))
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('title', title)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            item_loader.add_value('rent_string', price)
            item_loader.add_xpath('description', '//h3[contains(text(), "Description")]/following-sibling::p//text()')
            item_loader.add_value('square_meters', square_meters)
            item_loader.add_value('images', images)
            if terrace:
                item_loader.add_value('terrace', True)
            if elevator:
                item_loader.add_value('elevator', True)
            if parking:
                item_loader.add_value('parking', True)
            if dishwasher:
                item_loader.add_value('dishwasher', True)
            item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('landlord_name', 'Service Acor')
            item_loader.add_value('landlord_email', 'service.acor@gmail.com')
            item_loader.add_value('landlord_phone', '+32 (0)71 32.02.08')
            item_loader.add_value('external_source', 'Serviceacor_PySpider_france_fr')
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('city', city)
            yield item_loader.load_item()



         