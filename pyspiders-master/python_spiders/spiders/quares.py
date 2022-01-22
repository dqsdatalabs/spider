# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re, json
from bs4 import BeautifulSoup
from python_spiders.helper import string_found
from ..loaders import ListingLoader
from scrapy import Request,FormRequest

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

class QuaresSpider(scrapy.Spider):
    name = 'quares' 
    allowed_domains = ['quares']
    start_urls = ['https://agency.quares.be/']
    execution_type = 'testing'
    country = 'belgium' 
    locale ='nl'
    thousand_separator=','
    scale_separator='.'
    external_source = 'Quares_PySpider_belgium_nl'

    def start_requests(self):
        start_urls = [
            {'url': 'https://immo.quares.be/nl/te-huur?category%5B%5D=7&minPrice=&maxPrice=&bedrooms=&neighbourhood=&reference=&keyword=',
                'property_type': 'apartment'},
        ]
        for url in start_urls:
            yield Request(url=url.get('url'),
                                    callback=self.parse,
                                    meta={'property_type': url.get('property_type')})

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@id="estate-list"]//div[contains(@class,"item")]')
        for link in links:
            
            url = response.urljoin(link.xpath('./@data-href').extract_first())
            print(url)
            if "https://immo.quares.be/nl/te-huur?category" in url:
                continue
            yield scrapy.Request(
                url=url,
                callback=self.get_property_details,
                meta={'property_type': response.meta.get('property_type')},
                dont_filter=True
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        soup = BeautifulSoup(response.body)
        temp_dic = {}
        if soup.find("div",id="detail-features"):
            
            all_tr=soup.find("div",id="detail-features").find_all("div",class_="item")
            for ech_tr in all_tr:
                if ech_tr.find("div",class_="left") and ech_tr.find("div",class_="right"):
                    key = ech_tr.find("div",class_="left").text.strip()
                    vals=ech_tr.find("div",class_="right").text.strip()
                    temp_dic.update({key:vals})
            temp_dic = cleanKey(temp_dic)
        if 'epc' in temp_dic:
            item_loader.add_value('energy_label', str(temp_dic["epc"]))    
        if "gemeenschappelijkelastenhuurder" in temp_dic:
            item_loader.add_value('utilities', getSqureMtr(temp_dic["gemeenschappelijkelastenhuurder"]))
        external_id = response.xpath('//p[contains(text(), "referentie")]/strong/text()').extract_first()
        externalid=item_loader.get_output_value("external_id")
        if not externalid:
            externalid=response.url.split("detail/")[-1].split("/")[0]
            if externalid:
                item_loader.add_value("external_id",externalid)
        check=response.xpath("//h1//text()").get()
        if check and "binnenstaanplaats" in check.lower():
            return
        external_link = response.url
        if external_link and "parking" in external_link:
            return 
        title = response.xpath('//meta[@property="og:title"]/@content').extract_first()
        property_type = response.meta.get('property_type')
        address = ''.join(response.xpath('//div[contains(@class, "detail-info-banner")]//img[contains(@alt, "Location")]/following-sibling::span/text()').extract())
        if address:
            zipcode, city = extract_city_zipcode(address)
            item_loader.add_value('zipcode', zipcode)
            item_loader.add_value('city', city)
        bathrooms = response.xpath('//div[@class="estate-detail-icons"]//img[contains(@alt, "Bathroom")]/following-sibling::span/text()').extract_first('')
        landlord_name = response.xpath('//div[contains(@class, "contact-info")]/h3/text()').extract_first()
        landlord_phone = response.xpath('//div[contains(@class, "contact-info")]/ul/li//img[contains(@alt, "Phone")]/../following-sibling::span/text()').extract_first()
        landlord_email = response.xpath('//div[contains(@class, "contact-info")]/ul/li//img[contains(@alt, "Mail")]/../following-sibling::span/text()').extract_first()
        lat_lon = re.search(r'new google\.maps\.LatLng\((.*?)\)', response.text).group(1)
        if ',' in lat_lon: 
            lat = lat_lon.split(', ')[0]
            lon = lat_lon.split(', ')[1]
        else:
            lat = ''
            lon = ''
        energy_label = response.xpath('//div[contains(text(), "EPC")]/following-sibling::div/text()').extract_first('')
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('title', title)
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        if lon:
            item_loader.add_value('longitude', str(lon))
        if lat:
            item_loader.add_value('latitude', str(lat)) 
        if bathrooms: 
            item_loader.add_value('bathroom_count', str(bathrooms))
        rent = response.xpath("//div[contains(text(), \"prijs\")]/following-sibling::div/text()").get()
        if rent: 
            item_loader.add_xpath("rent", "".join(filter(str.isnumeric, rent)))
        item_loader.add_value("currency", "EUR")
        item_loader.add_xpath('description', '//h2[contains(text(), "Beschrijving")]/../p//text()')
        item_loader.add_xpath('square_meters', '//img[contains(@title, "Opp")]/following-sibling::span/text()')
        item_loader.add_xpath('floor', '//div[text()="Verdieping"]/following-sibling::div/text()')
        item_loader.add_xpath('images', '//div[@class="estate-detail-slider"]/a/@href')
        terrace_texts = response.xpath('//h3[contains(text(), "Buiten")]/following-sibling::div/p/text()').extract_first('')
        if string_found(['terras'], terrace_texts):
            item_loader.add_value('terrace', True)
        parking_text = response.xpath('//h3[contains(text(), "Buiten")]/following-sibling::div/p/text()').extract_first('')
        if string_found(['garage'], parking_text):
            item_loader.add_value('parking', True)
        item_loader.add_xpath('room_count', '//img[@title="Slaapkamers"]/following-sibling::span/text()')
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_email', landlord_email)
        item_loader.add_value('landlord_phone', landlord_phone)
        item_loader.add_value('external_source', self.external_source)
     
        from datetime import datetime
        import dateparser
        available_date =response.xpath("//div[contains(.,'Beschikbaarheid (datum)')]/following-sibling::div/text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        yield item_loader.load_item()