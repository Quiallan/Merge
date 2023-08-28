require 'net/ftp'
require 'net/sftp'
require 'data_record'
require 'active_support'
require 'active_support/core_ext'
require 'fileutils'
require 'date'

week = 0.weeks
#LAST WEEK ( this is here so we can run last weeks data if it was missed...
# just uncomment the next line )
#week = 1.weeks

def join_record(record,header)
	return_array = []
	header.each do |column|
		return_array << '"' + record.send(column) + '"'
	end
	return_array
end
# DOWNLOAD all of last weeks files!
 date = Date.today
 puts start_date = date.last_week.beginning_of_week(:monday) - week
 puts end_date = date.last_week.end_of_week(:sunday) - week
 timestamp = "#{start_date.month}_#{start_date.day}-#{end_date.month}_#{end_date.day}_"
 month = Date::MONTHNAMES[end_date.month]

if !Dir.exist?(month)
	Dir.mkdir(month)
end

if !Dir.exist?(month + "/" + timestamp.chomp("_"))
	Dir.mkdir(month + "/" + timestamp.chomp("_"))
	Dir.mkdir(month + "/" + timestamp.chomp("_") + "/" + "customer_original")
	Dir.mkdir(month + "/" + timestamp.chomp("_") + "/" + "x-media")
	Dir.mkdir(month + "/" + timestamp.chomp("_") + "/" + "x-media/in")
	Dir.mkdir(month + "/" + timestamp.chomp("_") + "/" + "x-media/out")
end

file_prefix = ["rtl-ihst-h-","rtl-ihst-i-"]
dates = (start_date..end_date)
file_list = []
file_prefix.each do |pre|
	dates.to_a.each do |d|
		if d.monday? then next end
		file_list << pre + d.to_s.delete("-") + ".txt"
	end
end
#mX0dY8yE2zwS5n This is old old fileshare password. 
errors = false
ftp = Net::FTP.open('marketingtechfileshare.com')
ftp.login('fileshare@marketingtechfileshare.com','Iq$2;6#5n%@')
ftp.chdir("/marketingtechfileshare.com/public_html/repository/dunn-tire")
file_list.each do |file|
	begin
    	ftp.getbinaryfile(file)    	
  	rescue Net::FTPPermError => e
    	puts "Unable to download file #{file}"
    	errors = true
  	end
end
if errors == true
	puts "Terminating Program not all files have been downloaded!"
	exit
end
# Delete all files
file_list.each do |file|
	ftp.delete(file)
end

ftp.close

# h & i files - same headers for each differnt one.
# 	- combine into 2 files with headers
h_files = []
i_files = []
files = file_list.each do |file|
	if file.include?("-h-") then h_files << file end
	if file.include?("-i-") then i_files << file end
end
h_header = "Customer Number	PURL	Location ID	ServiceDate	First Name	Last Name	Address	City	State	Zip	Phone 1	Phone 2	Email	Vehicle/Delivery	License Plate	License Plate State	Vehicle Mileage	Merch Sub-Total	Sales Tax	State Tire Fee	FET	Type	Customer PO Number	Junk"
i_header = "PURL	InvoiceLineNumber	Part Number	Description	Detailed Size	Sort Key (Size)	Season	Brand Key	Product Line Key	J	Returns	Junk1	Junk2"
h_out = File.new("h_files.txt","w:UTF-8")
h_out.puts(h_header)
i_out = File.new("i_files.txt","w:UTF-8")
i_out.puts(i_header)
h_files.each do |file|
	h_out.write(File.read(file, :encoding => 'UTF-8'))
end
i_files.each do |file|
	i_out.write(File.read(file, :encoding => 'UTF-8'))
end
h_out.close
i_out.close
report = Hash.new([])
# Open the H files, remove duplicate PURLS, and Bad Email Addresses.
VALID_EMAIL_REGEX = /\A[\w+\-.]+@[a-z\d\-]+(\.[a-z]+)*\.[a-z]+\z/i
d = DataRecord.new
data = d.read("h_files.txt","\t",["plus_one_year","servicedatevariable","coupon_90","coupon_120","coupon_190","coupon_330","coupon_351","coupon_700","coupon_1080"])
uniq_array = []  #VALID EMAIL ADDRESSES
emails = []
emails_rejects = ["NO","NOEMAIL","NOEMAIL2","NOEMAILGIVEN","NOMAIL","NONE","NOO","NOTHANKS","NOTEMAIL","OOPS"]
domain_rejects = ["NOEMAIL.COM"]
domains = ["AOL.COM","GMAIL.COM","YAHOO.COM","MSN.COM","HOTMAIL.COM","ROADRUNNER.COM","MAIL.COM"]

data.each do |rec|
	return if !data
	begin
	date = Date.parse(rec.ServiceDate)
			date = Date.parse(rec.ServiceDate)
	rescue
			date =  Date.today
	end
	rec.coupon_90 = ((date + 90) + 1.month).strftime('%m/%d/%Y')
	rec.coupon_120 = ((date + 120) + 1.month).strftime('%m/%d/%Y')
	rec.coupon_190 = ((date + 190) + 1.month).strftime('%m/%d/%Y')
	rec.coupon_330 = ((date + 330) + 1.month).strftime('%m/%d/%Y')
	rec.coupon_351 = ((date + 351) + 1.month).strftime('%m/%d/%Y')
	rec.coupon_700 = ((date + 700) + 1.month).strftime('%m/%d/%Y')
	rec.coupon_1080 = ((date + 1080) + 1.month).strftime('%m/%d/%Y')
	if rec.Location_ID == "1090" || rec.Location_ID == "1091" || rec.Location_ID == "2092" then next end
	if rec.Email.downcase.strip == "dfarrell107@gmail.com" then next end
	if rec.Email.strip =~ VALID_EMAIL_REGEX && uniq_array.include?(rec.PURL) == false
		e = rec.Email.upcase.split("@")
		if e[0].size > 1 && !emails_rejects.include?(e[0]) && !domain_rejects.include?(e[1])
			emails << rec
			uniq_array << rec.PURL
		elsif e[0].size == 1 && !domains.include?(e[1]) && e[0] =~ /d/
			emails << rec
			uniq_array << rec.PURL
		else 
			rec.Email = ""
		end
		date = Date.parse(rec.ServiceDate)
		rec.ServiceDate = date.strftime("%m/%d/%Y")
		rec.servicedatevariable = rec.ServiceDate
		rec.plus_one_year = (date + 1.year).end_of_month.strftime("%m/%d/%Y")
	end
	report[rec.Location_ID] += [rec]
end
# output all good data to emails_1.txt without the Email Column
File.open("#{timestamp}emails_1.csv","w") do |f|
	f.puts (d.header - ["Email"]).join(",")
	emails.each do |rec|
		f.puts(join_record(rec,d.header - ["Email"]).join(","))
	end
end


# output all good data to email_2.txt with just the PURL & Email columns
h2 = ["PURL","Email"]
File.open("#{timestamp}emails_2.csv","w") do |f|
	f.puts h2.join(",")
	emails.each do |rec|
		f.puts(join_record(rec,h2).join(","))
	end
end

# Create Weekly Report
report_out = File.open("#{timestamp}report.csv","w")
report_header = ["store","no_email","no_partic","unique_emails","invoices","invoice_percent","customers","customer_percent"]
report_out.puts report_header.join(",")

no_email_count,no_partic_count,uniq_email_count,invoices_count,customer_count,invoice_percent_count,customer_percent_count = 0,0,0,0,0,0,0

report.sort.each do |location|
	# dedupe by customer number
	uniq_customers = location[1].uniq(&:Customer_Number)
	uniq_invoices = location[1].uniq(&:PURL)
	uniq_email = location[1].uniq(&:Email).delete_if{|e| !e.Email.include?("@")}
	no_email = location[1].uniq(&:PURL).delete_if{|e| !["#1",""].include?(e.Email.strip)}
	not_participate = location[1].uniq(&:PURL).delete_if{|e| !["#2","#3"].include?(e.Email.strip)}
	lr = []
	lr << location[0]
	lr << no_email.size.to_s
	lr << not_participate.size.to_s
	lr << uniq_email.size.to_s
	lr << uniq_invoices.size.to_s
	lr << (uniq_email.size.to_f / uniq_invoices.size.to_f * 100.0).round(2).to_s + "%"
	lr << uniq_customers.size.to_s
	lr << (uniq_email.size.to_f / uniq_customers.size.to_f * 100.0).round(2).to_s + "%"
	report_out.puts(lr.join(","))
	no_email_count += no_email.size
	no_partic_count += not_participate.size
	uniq_email_count += uniq_email.size
	invoices_count += uniq_invoices.size
	customer_count += uniq_customers.size
	invoice_percent_count += (uniq_email.size.to_f / uniq_invoices.size.to_f * 100.0).round(2)
	customer_percent_count += (uniq_email.size.to_f / uniq_customers.size.to_f * 100.0).round(2)
end
report_out.puts ",#{no_email_count},#{no_partic_count},#{uniq_email_count},#{invoices_count},#{(uniq_email_count.to_f/invoices_count.to_f*100.0).round(2)}%,#{customer_count},#{(uniq_email_count.to_f/customer_count.to_f*100.0).round(2)}%"
report_out.close

# Open the I files, pull out Tire, Rotation, Alignment, Inspeciton, and Oilchange
# create new files for each.

d = DataRecord.new
data = d.read("i_files.txt","\t")

# setup arrays for each of the file types.

tires = []
rotation = []
alignment = []
oilchange = []
inspection = []
transmission = []
brake_fluid = []
power_steering = []
fuel_systems = []
differential = []

# inspection_purls = []

data.each do | rec|
	if rec.J == "T" && uniq_array.include?(rec.PURL) then tires << rec end
	if rec.J == "R" && uniq_array.include?(rec.PURL) then rotation << rec end
	if rec.J == "G" && uniq_array.include?(rec.PURL) then alignment << rec end
	if rec.J == "H" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key == "OILCHA" then oilchange << rec end
	if rec.J == "H" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key == "INSPEC" then inspection << rec end
	if rec.J == "H" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key == "PSFLUS" then power_steering << rec end
	if rec.J == "H" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key == "TFLUSH" then transmission << rec end
	if rec.J == "f" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key == "BFLUSH" then brake_fluid << rec end
	if rec.J == "f" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key == "DIFFFL" then differential << rec end
	if rec.J == "H" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key == "FUELSV" then fuel_systems << rec end
	if rec.J == "H" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key.strip == "MISC" && rec.Sort_Key__Size_.strip == "INSPECTION" then inspection << rec end
	if rec.J == "H" && uniq_array.include?(rec.PURL) && rec.Product_Line_Key.strip == "MISC" && rec.Sort_Key__Size_.strip == "STICKER" then inspection << rec end
end

tires.uniq!(&:PURL)
rotation.uniq!(&:PURL)
alignment.uniq!(&:PURL)
oilchange.uniq!(&:PURL)
inspection.uniq!(&:PURL)
transmission.uniq!(&:PURL)
brake_fluid.uniq!(&:PURL)
power_steering.uniq!(&:PURL)
fuel_systems.uniq!(&:PURL)


tire_header = "PURL,Part_Number,Description,Detailed_Size,Sort_Key__Size_,Season,Brand_Key,Product_Line_Key,TIRE".split(",")
File.open("#{timestamp}tires.csv","w") do |f|
	f.puts tire_header.join(",")
	tires.each do |rec|
		tire_header.each do |h|
			f.write rec.send(h) unless h == "TIRE"
			if h == "TIRE" 
				f.puts "TRUE"
			else
				f.write ","
			end
		end
	end
end

File.open("#{timestamp}rotation.csv","w") do |f|
	f.puts "PURL,ROTATION,FREE"
	rotation.each do |rec|
		f.write rec.PURL + "," 
		f.write "TRUE,"
		if rec.Detailed_Size.strip == "FREE"
			f.puts "TRUE"
		else
			f.puts
		end
	end
end

File.open("#{timestamp}alignment.csv","w") do |f|
	f.puts "PURL,ALIGNMENT"
	alignment.each do |rec|
		f.puts rec.PURL + ",TRUE"
	end
end

File.open("#{timestamp}oilchange.csv","w") do |f|
	f.puts "PURL,OILCHANGE"
	oilchange.each do |rec|
		f.puts rec.PURL + ",TRUE"
	end
end

File.open("#{timestamp}inspection.csv","w") do |f|
	f.puts "PURL,INSPECTION"
	inspection.each do |rec|
		f.puts rec.PURL + ",TRUE"
	end
end

File.open("#{timestamp}transmission.csv","w") do |f|
	f.puts "PURL,TRANSMISSION"
	transmission.each do |rec|
		f.puts rec.PURL + ",TRUE"
	end
end

File.open("#{timestamp}breakfluid.csv","w") do |f|
	f.puts "PURL,BREAKFLUID"
	brake_fluid.each do |rec|
		f.puts rec.PURL + ",TRUE"
	end
end

File.open("#{timestamp}differential.csv","w") do |f|
	f.puts "PURL,DIFFERENTIAL"
	differential.each do |rec|
		f.puts rec.PURL + ",TRUE"
	end
end

File.open("#{timestamp}powersteering.csv","w") do |f|
	f.puts "PURL,POWERSTEERING"
	power_steering.each do |rec|
		f.puts rec.PURL + ",TRUE"
	end
end

File.open("#{timestamp}fuelsystems.csv","w") do |f|
	f.puts "PURL,FUELSYSTEMS"
	fuel_systems.each do |rec|
		f.puts rec.PURL + ",TRUE"
	end
end

# Cleanup -- move all of the downloaded files, reports, and mindfire data.
file_list += ["h_files.txt","i_files.txt"]
path = month + "/" + timestamp.chomp("_") + "/" + "customer_original/"
file_list.each do |file|
	FileUtils.mv(file,path + file)
end
xmedia_files = Dir.glob("*.csv")
path = month + "/" + timestamp.chomp("_") + "/" + "x-media/in/"
xmedia_files.each do |file|
	FileUtils.mv(file,path + file)
end
puts "Emails:\t\t" + uniq_array.size.to_s
puts "Tires:\t\t" + tires.size.to_s
puts "Rotation:\t" + rotation.size.to_s
puts "Alignment:\t" + alignment.size.to_s
puts "Oilchange:\t" + oilchange.size.to_s
puts "Inspeciton:\t" + inspection.size.to_s
puts "Transmission:\t" + transmission.size.to_s
puts "Brake Fluid:\t" + brake_fluid.size.to_s
puts "Differentail Service:\t" + differential.size.to_s
puts "Power Steering:\t" + power_steering.size.to_s
puts "Fuel Systems:\t" + fuel_systems.size.to_s                      