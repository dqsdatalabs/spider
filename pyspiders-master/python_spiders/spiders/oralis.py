# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re, json
from bs4 import BeautifulSoup
from ..loaders import ListingLoader
import dateparser
from python_spiders.helper import remove_unicode_char, extract_rent_currency, format_date
import unicodedata

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

class OralisSpider(scrapy.Spider):
    name = "oralis"
    allowed_domains = ["www.oralis.be"]
    start_urls = (
        'http://www.www.oralis.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.oralis.be/fr/List/InfiniteScroll?json=%7B%0A%20%20%22SliderList%22%3A%20false,%0A%20%20%22IsProject%22%3A%20false,%0A%20%20%22PageMaximum%22%3A%200,%0A%20%20%22FirstPage%22%3A%20false,%0A%20%20%22CanGetNextPage%22%3A%20false,%0A%20%20%22CMSListType%22%3A%202,%0A%20%20%22SortParameter%22%3A%205,%0A%20%20%22MaxItemsPerPage%22%3A%2012,%0A%20%20%22PageNumber%22%3A%200,%0A%20%20%22EstateSearchParams%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22StatusIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowDetails%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowRepresentatives%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CanHaveChildren%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20false%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CategoryIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%202%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D%0A%20%20%5D,%0A%20%20%22CustomQuery%22%3A%20null,%0A%20%20%22jsonEstateParams%22%3A%20null,%0A%20%20%22BaseEstateID%22%3A%200%0A%7D', 'property_type': 'apartment'},
            {'url': 'https://www.oralis.be/fr/List/InfiniteScroll?json=%7B%0A%20%20%22SliderList%22%3A%20false,%0A%20%20%22IsProject%22%3A%20false,%0A%20%20%22PageMaximum%22%3A%200,%0A%20%20%22FirstPage%22%3A%20true,%0A%20%20%22CanGetNextPage%22%3A%20false,%0A%20%20%22CMSListType%22%3A%202,%0A%20%20%22SortParameter%22%3A%205,%0A%20%20%22MaxItemsPerPage%22%3A%2012,%0A%20%20%22PageNumber%22%3A%200,%0A%20%20%22EstateSearchParams%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22StatusIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowDetails%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowRepresentatives%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CanHaveChildren%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20false%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CategoryIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D%0A%20%20%5D,%0A%20%20%22CustomQuery%22%3A%20null,%0A%20%20%22jsonEstateParams%22%3A%20null,%0A%20%20%22BaseEstateID%22%3A%200%0A%7D', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        if 'apartment' in response.meta.get('property_type'):
            for page in range(0, 7):
                url = 'https://www.oralis.be/fr/List/InfiniteScroll?json=%7B%0A%20%20%22SliderList%22%3A%20false,%0A%20%20%22IsProject%22%3A%20false,%0A%20%20%22PageMaximum%22%3A%200,%0A%20%20%22FirstPage%22%3A%20false,%0A%20%20%22CanGetNextPage%22%3A%20false,%0A%20%20%22CMSListType%22%3A%202,%0A%20%20%22SortParameter%22%3A%205,%0A%20%20%22MaxItemsPerPage%22%3A%2012,%0A%20%20%22PageNumber%22%3A%20{},%0A%20%20%22EstateSearchParams%22%3A%20%5B%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22StatusIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%201%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowDetails%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22ShowRepresentatives%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20true%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CanHaveChildren%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20false%0A%20%20%20%20%7D,%0A%20%20%20%20%7B%0A%20%20%20%20%20%20%22FieldName%22%3A%20%22CategoryIDList%22,%0A%20%20%20%20%20%20%22FieldValue%22%3A%20%5B%0A%20%20%20%20%20%20%20%202%0A%20%20%20%20%20%20%5D%0A%20%20%20%20%7D%0A%20%20%5D,%0A%20%20%22CustomQuery%22%3A%20null,%0A%20%20%22jsonEstateParams%22%3A%20null,%0A%20%20%22BaseEstateID%22%3A%200%0A%7D'.format(page)         
                yield scrapy.Request(url=url, callback=self.get_property_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        else:
            yield scrapy.Request(url=response.url, callback=self.get_property_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
    
    def get_property_urls(self, response):
        links = response.xpath('//div[@class="estate-list__item "]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
            
    def get_property_details(self, response):
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//th[contains(text(), "Référence")]/following-sibling::td/text()').extract_first().strip()
        city_zipcode = response.xpath('//meta[@property="og:title"]/@content').extract_first().split(' - ')[-1]
        address = ' rue Defacqz 40' + ' ' + city_zipcode  
        zipcode = re.findall(r'\d+', city_zipcode)[0]
        city = city_zipcode.replace(zipcode, '')  
        room_count = response.xpath('//th[contains(text(), "chambres")]/following-sibling::td/text()').extract_first('').strip()
        square_meters = ''.join(response.xpath('//th[contains(text(), "habitable")]/following-sibling::td/text()').extract())
        bathrooms = response.xpath('//img[@alt="nb_bath"]/following-sibling::p/text()').extract_first('').strip()
        item_loader = ListingLoader(response=response)
        soup = BeautifulSoup(response.body)
        if soup.find("div", class_="row affix-container"):
            temp_dic = {}
            all_tr=soup.find("div", class_="row affix-container").find_all("tr")
            for ech_tr in all_tr:
                if ech_tr.find("th",class_="estate-table__label") and ech_tr.find("td",class_="estate-table__value"):
                    key = ech_tr.find("th",class_="estate-table__label").text.strip()
                    vals=ech_tr.find("td",class_="estate-table__value").text.strip()
                    temp_dic.update({key:vals})
            temp_dic = cleanKey(temp_dic)
        if 'disponibilit' in temp_dic:
            item_loader.add_value( "available_date", format_date(temp_dic['disponibilit']))
        if 'nombredesallesdebain' in temp_dic:
            item_loader.add_value('bathroom_count', str(temp_dic['nombredesallesdebain'])) 
        if 'peb_etotale_kwh_an' in temp_dic:
            item_loader.add_value('energy_label', str(temp_dic['peb_etotale_kwh_an']))
        if 'piscine' in temp_dic:
            if 'Non' not in temp_dic['piscine']:
                item_loader.add_value('swimming_pool', True)
            else:
                item_loader.add_value('swimming_pool', False)
        else:
            item_loader.add_value('swimming_pool', False)
        if 'meubl' in temp_dic:
            if 'Non' not in temp_dic['meubl']:
                item_loader.add_value('furnished', True)
            else:
                item_loader.add_value('furnished', False)
        else:
            item_loader.add_value('furnished', False)
        if 'lift' in temp_dic:
            if 'ja' in temp_dic['lift']:
                item_loader.add_value('elevator', True)
            else:
                item_loader.add_value('elevator', False)
        else:
            item_loader.add_value('elevator', False)
        if 'parking' in temp_dic:
            if 'Oui' in temp_dic['parking']:
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)
        else:
            item_loader.add_value('parking', False)
        
        price = "".join(
            response.xpath(
                "//h1[@class='line-separator-after h2 estate-detail-intro__text']/text()[normalize-space()][contains(., '€')]"
            ).extract()
        )
        if price:
            p = unicodedata.normalize("NFKD", price)
            item_loader.add_value("rent", p.split("€")[0].replace(" ", ""))
            item_loader.add_value("currency", "EUR")
        
        utilities =response.xpath(
                "//th[contains(.,'Charge')]/following-sibling::td/text()"
            ).get()
        if utilities:
            item_loader.add_value("utilities", utilities.strip())

        deposit = response.xpath("//th[contains(.,'Garantie')]/following-sibling::td/text()").get()
        if deposit:
            item_loader.add_value("deposit",int(item_loader.get_collected_values("rent")[0])*int(deposit))
        
        item_loader.add_value('property_type', property_type)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', str(external_id))
        item_loader.add_xpath('title', '//meta[@property="og:title"]/@content')
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//h2[contains(text(), "Description")]/following-sibling::p//text()')
        item_loader.add_xpath('images', '//img[@class="owl-estate-photo__img"]/@src')
        item_loader.add_value('square_meters', square_meters)
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('bathroom_count', str(bathrooms))
        
        if not item_loader.get_collected_values("bathroom_count"):
            bathroom_count = response.xpath("//div[contains(@class,'estate-detail')]//i[contains(@class,'fa-bath')]/following-sibling::text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        item_loader.add_value('landlord_name', 'ORALIS')
        item_loader.add_value('landlord_email', 'info@oralis.be')
        item_loader.add_value('landlord_phone', '+32 2 533 40 40')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()