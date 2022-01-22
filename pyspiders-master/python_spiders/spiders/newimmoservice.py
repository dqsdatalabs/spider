# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re, json
from bs4 import BeautifulSoup
from ..loaders import ListingLoader
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode = zip_city.split(" ")[0]
    city = zip_city.replace(' ' + zipcode, '') 
    return zipcode, city

def extract_city_zipcode(_address):
    if len(_address.split(", ")) < 2:
       address = "Museumstraat 50, " + _address
    else:
        address = _address
    zip_city = address.split(", ")[1]
    if len(zip_city.split(" ")) > 2:
        zipcode, city = (zip_city.split(" ")[0], zip_city.replace(zip_city.split(" ")[0], ''))
    else:
        zipcode, city = zip_city.split(" ")
    return zipcode, city
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

class NewimmoserviceSpider(scrapy.Spider):
    name = 'newimmoservice'
    allowed_domains = ['newimmoservice']
    start_urls = ['https://www.newimmoservice.be/nl/te-huur/']
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.newimmoservice.be/nl/te-huur/appartementen/',
                'property_type': 'apartment'},
            {'url': 'https://www.newimmoservice.be/nl/te-huur/woningen/',
                'property_type': 'apartment'}
            
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        for link in response.xpath('//ul[@id="properties"]/article[@class="property"]/a[@class="property-content"]'):
            property_url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'property_type': response.meta.get('property_type')},
                dont_filter=True
            )
    
    def get_property_details(self, response):
        external_id = ''.join(response.xpath('//dt[contains(text(), "Referentie")]/following-sibling::dd//text()').extract()).strip()
        if external_id: 
            item_loader = ListingLoader(response=response)
            external_link = response.url
            address = response.xpath('//header[@id="top"]/div[@class="address"]/text()').extract_first('')
            property_type = response.meta.get('property_type')
            zipcode, city = extract_city_zipcode(address)
            title = response.xpath('//meta[@property="og:title"]/@content').extract_first()
            title = re.sub(r'\t\n', '', title)
            bathrooms = response.xpath('//header[@id="top"]//li[@class="bathrooms"]/text()').extract_first('').strip()
            if bathrooms: 
                item_loader.add_value('bathroom_count', str(bathrooms))
            details_text = ''.join(response.xpath('//meta[@property="og:description"]/@content').extract())
            images = []
            image_links = response.xpath('//ul[@id="photos"]/li//img')
            for image_link in image_links:
                image_url = image_link.xpath('./@src').extract_first()
                if image_url not in images:
                    images.append(image_url)
            soup = BeautifulSoup(response.body)
            if soup.find("article", id="details"):
                temp_dic = {}
                all_tr=soup.find("article",id="details").find_all("dl")
                for ech_tr in all_tr:
                    if ech_tr.find("dt") and ech_tr.find("dd"):
                        key = ech_tr.find("dt").text.strip()
                        vals=ech_tr.find("dd").text.strip()
                        temp_dic.update({key:vals})
                temp_dic = cleanKey(temp_dic)
            
            if 'epc' in temp_dic:
                item_loader.add_value('energy_label', str(temp_dic["epc"]))    
            if "beschikbaarheid" in temp_dic and 'onmiddellijk' not in temp_dic['beschikbaarheid'].lower():
                item_loader.add_value('available_date', format_date(temp_dic['beschikbaarheid']))
            elevator_va = response.xpath('//article[@id="details"]//dt[contains(text(), "Lift")]/following-sibling::dd/span/text()').extract_first('')
            if "Ja" in elevator_va:
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)
            utilities = response.xpath('//article[@id="details"]//dt[contains(text(), "Kosten")]/following-sibling::dd/span/text()').extract_first('').strip()
            if utilities:
                item_loader.add_value('utilities', getSqureMtr(utilities)) 
            floor_text = response.xpath('//dt[contains(text(), "Verdie")]/following-sibling::dd/span/text()').extract_first('')
            if floor_text:
                floor = re.sub(r'[\n\t]+', '', floor_text)
            else:
                floor = ''
            if 'garage' in details_text.lower() or 'parking' in details_text.lower():
                parking = True
            else:
                parking = ''
            try:
                terrace_text = response.xpath('//dt[contains(text(), "Terras")]/following-sibling::dd/span/text()').extract_first().replace('\t', '').replace('\n', '')
                if terrace_text:
                    terrace = True
                else:
                    terrace = ''
            except:
                terrace = ''
            lat = re.search(r'var lat\s=\s(.*?)\;', response.text).group(1)
            lon = re.search(r'var lng\s=\s(.*?)\;', response.text).group(1)
            rent_text = ''.join(response.xpath('//dt[contains(text(), "Prijs")]/following-sibling::dd/span/text()').extract())
            rent = re.sub(r'[\s]+', '', rent_text)
            furnished = response.xpath("//dt[contains(.,'Gemeubeld')]//following-sibling::dd[contains(.,'Ja')]//text()").get()
            if furnished:
                item_loader.add_value("furnished", True)
            prop_type = response.xpath("//h2[contains(.,'Studio')]//text()").get()
            if prop_type:
                item_loader.add_value("property_type", "studio")
            else:
                item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_id', external_id)
            item_loader.add_value('title', title)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('address', address)
            item_loader.add_value('rent_string', rent)
            item_loader.add_xpath('description', '//meta[@property="og:description"]/@content')
            item_loader.add_xpath('square_meters', '//header[@id="top"]//li[@class="area"]/text()')
            if floor:
                item_loader.add_value('floor', floor)
            item_loader.add_value('images', images)
            if parking:
                item_loader.add_value('parking', True)
            if terrace:
                item_loader.add_value('terrace', True)
            item_loader.add_value('longitude', str(lon))
            item_loader.add_value('latitude', str(lat))
            item_loader.add_xpath('room_count', '//header[@id="top"]//li[@class="rooms"]/text()')
            item_loader.add_value('landlord_name', 'NEW IMMO SERVICE')
            item_loader.add_value('landlord_email', 'info@newimmoservice.be')
            item_loader.add_value('landlord_phone', '02 305 30 03')
            item_loader.add_value('external_source', 'Newimmoservice_PySpider_belgium_nl')
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('city', city)
            yield item_loader.load_item()